"""Daily split-adjusted bars (index = bar start, Bangkok time) from TradingView's chart websocket (anonymous), many symbols per connection."""
import hashlib
import json
import re
import time
from datetime import datetime, timedelta, timezone

import pandas as pd

ICT = timezone(timedelta(hours=7))


def _frame(func, params):
    m = json.dumps({"m": func, "p": params}, separators=(",", ":"))
    return f"~m~{len(m)}~m~{m}"


def tv_daily(symbols: list[str], n: int = 8000, timeout: float = 60, adjustment: str = "splits") -> dict:
    """{symbol: DataFrame(open, high, low, close, volume) indexed by Bangkok date} or {symbol: error str}."""
    from websockets.sync.client import connect

    out = {}
    with connect("wss://data.tradingview.com/socket.io/websocket?type=chart",
                 additional_headers={"Origin": "https://www.tradingview.com"},
                 open_timeout=20, max_size=None) as ws:
        ws.send(_frame("set_auth_token", ["unauthorized_user_token"]))
        for sym in symbols:
            cs = "cs_" + hashlib.sha1(f"{sym}{time.time()}".encode()).hexdigest()[:12]
            ws.send(_frame("chart_create_session", [cs, ""]))
            ws.send(_frame("resolve_symbol", [cs, "sym1", "=" + json.dumps({"symbol": sym, "adjustment": adjustment})]))
            ws.send(_frame("create_series", [cs, "s1", "s1", "sym1", "1D", n]))
            got, deadline = [], time.time() + timeout
            while sym not in out and time.time() < deadline:
                for msg in re.split(r"~m~\d+~m~", ws.recv(timeout=timeout))[1:]:
                    if msg.startswith("~h~"):
                        ws.send(f"~m~{len(msg)}~m~{msg}")
                        continue
                    try:
                        j = json.loads(msg)
                    except ValueError:
                        continue
                    m, p = j.get("m"), j.get("p") or [None]
                    if p[0] != cs:
                        continue
                    if m == "timescale_update":
                        got += p[1].get("s1", {}).get("s", [])
                    elif m == "series_completed":
                        rows = [(b["v"] + [None] * 6)[:6] for b in got]
                        df = pd.DataFrame(rows, columns=["t", "open", "high", "low", "close", "volume"])
                        # keep the Bangkok timestamp: night-session bars (DRs, 19:00 start) belong
                        # to the NEXT trading day - the caller maps them
                        df.index = pd.to_datetime([datetime.fromtimestamp(t, ICT).replace(tzinfo=None) for t in df.pop("t")])
                        out[sym] = df[~df.index.duplicated(keep="last")].sort_index()
                    elif m in ("symbol_error", "series_error", "critical_error", "protocol_error"):
                        out[sym] = f"{m} {p}"
            out.setdefault(sym, "timeout")
            ws.send(_frame("chart_delete_session", [cs]))
    return out
