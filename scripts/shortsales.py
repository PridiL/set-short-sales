# -*- coding: utf-8 -*-
"""SET short-sales dashboard data builder.

Source: https://www.set.or.th/th/market/statistics/short-sales/total-short-sales
  - /api/set/shortsales/statistics/list?fromDate=DD/MM/YYYY&toDate=DD/MM/YYYY  (per-stock table)
  - /api/set/shortsales/trading/summary?fromDate=..&toDate=..                  (market total)
  - /api/set/shortsales/allowed-period                                          (SET keeps ~6 months)
  The site sits behind Incapsula, so every call runs inside a headless Chromium page.

Prices: TradingView daily bars (anonymous chart websocket), SET:<symbol>.

Per trading date D, written to <data>/<D>.json:
  - the SET table as published (Local / NVDR / Total short volume, value, % value,
    outstanding Local / NVDR / Total, % outstanding of paid-up shares)
  - chg    : price % change on D  (close_D / previous close - 1)
  - dOS    : change in total outstanding short position vs the previous trading date
  - adv20  : average daily volume of the 20 sessions ending on D (TradingView)
  - dtc    : Days to Cover = total outstanding short position / adv20

Usage: python shortsales.py <data dir> [<raw dir>] [--rebuild]
  Fetches every trading date inside SET's allowed period that is not yet in <data dir>
  (the first run backfills ~6 months); --rebuild recomputes all dates.
"""
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(Path(__file__).resolve().parent))
from tvfeed import tv_daily  # noqa: E402

ICT = timezone(timedelta(hours=7))
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
PAGE = "https://www.set.or.th/th/market/statistics/short-sales/total-short-sales"
ADV_N = 20

FIELDS = {  # SET API name -> short key in our JSON
    "localVolume": "lv", "nvdrVolume": "nv", "totalVolume": "tv",
    "totalValue": "val", "percentValue": "pv",
    "localShortPosition": "los", "nvdrShortPosition": "nos",
    "totalShortPosition": "tos", "percentShortPosition": "pos",
}


def d2s(d: date) -> str:
    return d.strftime("%d/%m/%Y")


def iso(s: str) -> date:
    return datetime.fromisoformat(s).date()


def fetch_set(want_dates, page_fetch):
    """Return {date: payload} for each requested date SET actually has data for."""
    got = {}
    for d in want_dates:
        q = f"?fromDate={d2s(d)}&toDate={d2s(d)}"
        lst = page_fetch("/api/set/shortsales/statistics/list" + q)
        if not isinstance(lst, dict) or not lst.get("shortSales"):
            print(f"  {d}: no rows"); continue
        if iso(lst["tradingBeginDate"]) != d:
            print(f"  {d}: SET answered {lst['tradingBeginDate'][:10]} - skipped"); continue
        summ = page_fetch("/api/set/shortsales/trading/summary" + q)
        got[d] = {"list": lst, "summary": summ}
        print(f"  {d}: {len(lst['shortSales'])} rows")
    return got


def open_set():
    from playwright.sync_api import sync_playwright
    pw = sync_playwright().start()
    browser = pw.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
    ctx = browser.new_context(user_agent=UA, locale="th-TH", viewport={"width": 1600, "height": 1000})
    page = ctx.new_page()
    page.goto(PAGE, wait_until="domcontentloaded", timeout=90000)
    page.wait_for_timeout(8000)

    def page_fetch(url, tries=3):
        for i in range(tries):
            try:
                return page.evaluate(
                    "u => fetch(u, {headers: {accept: 'application/json'}})"
                    ".then(r => { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })", url)
            except Exception as e:  # noqa: BLE001
                if i == tries - 1:
                    raise RuntimeError(f"{url}: {e}")
                page.wait_for_timeout(3000)

    def close():
        browser.close(); pw.stop()
    return page_fetch, close


def holidays(page_fetch, start: date, end: date) -> set:
    out, y, m = set(), start.year, start.month
    while (y, m) <= (end.year, end.month):
        for h in page_fetch(f"/api/set/stock-calendar/{y}/{m}/holiday?lang=th") or []:
            out.add(iso(h["date"]))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def bars_for(symbols, n, trading_days):
    """{symbol: DataFrame indexed by SET trading date} from TradingView, 6 sockets in parallel.

    A bar that starts in the evening (DR night session, 19:00) belongs to the next SET trading
    day; one with no known next trading day (today's bar still forming) is dropped.
    """
    import bisect
    import pandas as pd
    days = sorted(trading_days)
    syms = sorted(symbols)
    chunks = [syms[i::6] for i in range(6)]
    out = {}
    with ThreadPoolExecutor(6) as ex:
        for res in ex.map(lambda c: tv_daily(["SET:" + s for s in c], n=n, timeout=30), chunks):
            for k, v in res.items():
                out[k[4:]] = v
    bad = [s for s, v in out.items() if isinstance(v, str)]
    if bad:
        print(f"  TradingView had no bars for {len(bad)}: {' '.join(bad[:30])}{' ...' if len(bad) > 30 else ''}")
    good, shifted = {}, 0
    for s, df in out.items():
        if isinstance(df, str):
            continue
        idx = []
        for ts in df.index:
            d = ts.date()
            if ts.hour >= 17:
                j = bisect.bisect_right(days, d)
                d = days[j] if j < len(days) else None
                shifted += 1
            idx.append(pd.Timestamp(d) if d else pd.NaT)
        df = df.copy()
        df.index = pd.DatetimeIndex(idx)
        df = df[df.index.notna()]
        good[s] = df[~df.index.duplicated(keep="last")].sort_index()
    if shifted:
        print(f"  {shifted} evening-session bars mapped to the next trading day")
    return good


def price_stats(df, d):
    """(close, % change, adv20) for date d, or Nones when TradingView has no bar on d."""
    if df is None:
        return None, None, None
    import pandas as pd
    ts = pd.Timestamp(d)
    if ts not in df.index:
        return None, None, None
    upto = df.loc[:ts]
    close = float(upto["close"].iloc[-1])
    prev = float(upto["close"].iloc[-2]) if len(upto) > 1 else None
    chg = (close / prev - 1) * 100 if prev else None
    vol = upto["volume"].dropna().iloc[-ADV_N:]
    adv = float(vol.mean()) if len(vol) else None
    return close, chg, adv


def r2(x, n=2):
    return None if x is None else round(x, n)


def build_day(d, raw, prev_rows, bars):
    rows = []
    for r in raw["list"]["shortSales"]:
        row = {"s": r["symbol"]}
        for k, short in FIELDS.items():
            row[short] = r.get(k)
        tos = row["tos"] or 0
        if prev_rows is not None:
            p = prev_rows.get(r["symbol"])
            row["ptos"] = (p.get("tos") or 0) if p else 0
            row["dOS"] = tos - row["ptos"]
        close, chg, adv = price_stats(bars.get(r["symbol"]), d)
        row["close"], row["chg"], row["adv20"] = close, r2(chg), r2(adv, 0)
        row["dtc"] = r2(tos / adv) if adv and tos else (0 if adv else None)
        rows.append(row)
    rows.sort(key=lambda x: -(x["val"] or 0))
    s = raw.get("summary") or {}
    return {
        "date": d.isoformat(),
        "prevDate": None,
        "summary": {"value": s.get("value"), "percentValue": s.get("percentValue"),
                    "volume": s.get("volume"), "percentVolume": s.get("percentVolume")},
        "rows": rows,
        "generated": datetime.now(ICT).strftime("%Y-%m-%d %H:%M"),
    }


def main():
    data = Path(sys.argv[1]).resolve()
    rebuild = "--rebuild" in sys.argv
    # raw SET payloads: kept beside the site (not published) when a 2nd path is given,
    # otherwise in a temp folder - the built day files carry every field the page needs
    raw_dir = Path(sys.argv[2]).resolve() if len(sys.argv) > 2 and not sys.argv[2].startswith("--")         else data.parent / ".raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    data.mkdir(parents=True, exist_ok=True)
    built = {date.fromisoformat(p.stem) for p in data.glob("20*.json")}
    have = built | {date.fromisoformat(p.stem) for p in raw_dir.glob("*.json")}

    page_fetch, close = open_set()
    try:
        lo, hi = (iso(x) for x in page_fetch("/api/set/shortsales/allowed-period"))
        print(f"SET allowed period {lo} .. {hi}; have {len(have)} dates (latest {max(have) if have else '-'})")
        today = datetime.now(ICT).date()
        hol = holidays(page_fetch, lo, max(hi, today))
        expect = today - timedelta(days=1)
        while expect.weekday() >= 5 or expect in hol:
            expect -= timedelta(days=1)
        if hi < expect:
            print(f"WARNING: SET's latest short-sales date is {hi}, expected {expect}")
            if "--expect-fresh" in sys.argv:
                raise SystemExit(3)
        want, d = [], lo
        while d <= hi:
            if d.weekday() < 5 and d not in hol and d not in have:
                want.append(d)
            d += timedelta(days=1)
        print(f"fetching {len(want)} date(s)")
        new = fetch_set(want, page_fetch)
    finally:
        close()

    for d, raw in new.items():
        (raw_dir / f"{d}.json").write_text(json.dumps(raw, ensure_ascii=False), encoding="utf-8")

    raw_dates = {date.fromisoformat(p.stem) for p in raw_dir.glob("*.json")}
    all_dates = sorted(built | raw_dates)
    todo = [d for d in all_dates if d in raw_dates and (rebuild or d not in built or d in new)]
    if not todo:
        print("nothing new"); return 0

    span = (date.today() - todo[0]).days
    n_bars = int(span * 5 / 7) + ADV_N + 40
    symbols = set()
    for d in todo:
        raw = json.loads((raw_dir / f"{d}.json").read_text(encoding="utf-8"))
        symbols |= {r["symbol"] for r in raw["list"]["shortSales"]}
    print(f"TradingView bars: {len(symbols)} symbols x {n_bars}")
    bars = bars_for(symbols, n_bars, all_dates)

    def tos_on(day):
        f = data / f"{day}.json"
        if f.exists():
            return {r["s"]: {"tos": r.get("tos")} for r in json.loads(f.read_text(encoding="utf-8"))["rows"]}
        praw = json.loads((raw_dir / f"{day}.json").read_text(encoding="utf-8"))
        return {r["symbol"]: {"tos": r.get("totalShortPosition")} for r in praw["list"]["shortSales"]}

    for d in todo:
        i = all_dates.index(d)
        prev_rows = tos_on(all_dates[i - 1]) if i > 0 else None
        raw = json.loads((raw_dir / f"{d}.json").read_text(encoding="utf-8"))
        out = build_day(d, raw, prev_rows, bars)
        out["prevDate"] = all_dates[i - 1].isoformat() if i > 0 else None
        priced = sum(1 for r in out["rows"] if r["chg"] is not None and r["val"])
        shorted = sum(1 for r in out["rows"] if r["val"])
        out["priced"] = [priced, shorted]
        (data / f"{d}.json").write_text(json.dumps(out, ensure_ascii=False, separators=(",", ":")),
                                        encoding="utf-8")
        print(f"  built {d}: {shorted} shorted, {priced} priced")

    index = []
    for d in all_dates:
        f = data / f"{d}.json"
        if f.exists():
            j = json.loads(f.read_text(encoding="utf-8"))
            index.append({"date": j["date"], "value": j["summary"]["value"],
                          "pv": j["summary"]["percentValue"], "priced": j.get("priced")})
    (data / "dates.json").write_text(json.dumps({
        "dates": index[::-1],
        "generated": datetime.now(ICT).strftime("%Y-%m-%d %H:%M"),
    }, ensure_ascii=False), encoding="utf-8")
    print(f"dates.json: {len(index)} dates, latest {index[-1]['date']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
