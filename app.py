import os
from datetime import datetime
import psycopg
from dash import Dash, dcc, html, Input, Output, callback, no_update, dash_table
import plotly.graph_objects as go

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is missing. Set it in Render → Environment.")

def fetch_pairs():
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT pair FROM cryptoprices ORDER BY pair")
            return [r[0] for r in cur.fetchall()]

def fetch_data(pair: str, days: int):
    # try unquoted ma75 first; if fails, fallback to "MA75" AS ma75
    base = """
        SELECT time, pair, open, high, low, close, volume,
               avg_volume_20, rsi14, macd, macd_signal,
               bb_upper, bb_lower, bb_basis,
               ma50, ma100, ma200, sma10, sma50,
               vwma10, vwma20, vwma50,
               {ma75_expr}
        FROM cryptoprices
        WHERE pair = %s AND time >= NOW() - INTERVAL '%s days'
        ORDER BY time ASC
    """
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(base.format(ma75_expr="ma75"), (pair, days))
            except Exception:
                cur.execute(base.format(ma75_expr='"MA75" AS ma75'), (pair, days))
            rows = cur.fetchall()
            cols = [d.name for d in cur.description]
    # convert to dict-of-lists
    data = {c: [] for c in cols}
    for r in rows:
        for c, v in zip(cols, r):
            data[c].append(v)
    return data

app = Dash(__name__)
server = app.server

try:
    PAIRS = fetch_pairs()
except Exception:
    PAIRS = []

default_pair = "BTCUSDT" if "BTCUSDT" in PAIRS else (PAIRS[0] if PAIRS else None)

app.layout = html.Div(
    style={"maxWidth": "1100px", "margin": "0 auto", "padding": "18px"},
    children=[
        html.H2("📈 Crypto Dashboard (Neon → Dash)"),
        html.Div(style={"display":"flex","gap":"12px","flexWrap":"wrap"}, children=[
            html.Div([
                html.Label("Pair"),
                dcc.Dropdown(
                    id="pair",
                    options=[{"label": p, "value": p} for p in PAIRS],
                    value=default_pair, clearable=False, style={"minWidth":"220px"}
                )
            ]),
            html.Div(style={"flex":"1","minWidth":"320px"}, children=[
                html.Label("Days of history"),
                dcc.Slider(id="days", min=7, max=365, step=1, value=90,
                           marks={7:"7",30:"30",90:"90",180:"180",365:"365"})
            ])
        ]),
        html.Hr(),
        html.Div(id="kpis"),
        dcc.Graph(id="price_chart"),
        dcc.Graph(id="ma_chart"),
        dcc.Graph(id="macd_chart"),
        html.H3("Raw (latest 200)"),
        html.Div(id="table"),
        html.Div(id="footnote", style={"color":"#666","marginTop":"8px"})
    ]
)

@callback(
    Output("kpis","children"),
    Output("price_chart","figure"),
    Output("ma_chart","figure"),
    Output("macd_chart","figure"),
    Output("table","children"),
    Output("footnote","children"),
    Input("pair","value"),
    Input("days","value"),
)
def update(pair, days):
    if not pair:
        return html.Div("No pairs found."), no_update, no_update, no_update, no_update, ""

    data = fetch_data(pair, days)
    if not data.get("time"):
        return html.Div("No rows for selection."), no_update, no_update, no_update, no_update, ""

    time = data["time"]
    close = data["close"]
    volume = data.get("volume", [])

    # KPIs
    def fmt(x, d=6):
        try: return f"{float(x):.{d}f}"
        except: return "—"
    latest_idx = -1
    kpis = html.Div(style={"display":"grid","gridTemplateColumns":"repeat(4,1fr)","gap":"12px"}, children=[
        html.Div([html.Div("Close", style={"color":"#666"}), html.H3(fmt(close[latest_idx],6))]),
        html.Div([html.Div("RSI(14)", style={"color":"#666"}), html.H3(fmt(data.get("rsi14",[None])[latest_idx],2))]),
        html.Div([html.Div("MACD", style={"color":"#666"}), html.H3(fmt(data.get("macd",[None])[latest_idx],6))]),
        html.Div([html.Div("Volume", style={"color":"#666"}), html.H3(fmt(volume[latest_idx],0) if volume else "—")]),
    ])

    # Price chart
    fig_price = go.Figure()
    fig_price.add_trace(go.Scatter(x=time, y=close, mode="lines", name="Close"))
    fig_price.update_layout(title=f"Price • {pair}", margin=dict(l=20,r=20,t=40,b=20))

    # MA/Bands
    fig_ma = go.Figure()
    def add_series(name):
        if name in data and any(v is not None for v in data[name]):
            fig_ma.add_trace(go.Scatter(x=time, y=data[name], mode="lines", name=name.upper()))
    for c in ["sma10","sma50","ma50","ma75","ma100","ma200","bb_upper","bb_lower","bb_basis"]:
        add_series(c)
    fig_ma.update_layout(title="Moving Averages / Bands", margin=dict(l=20,r=20,t=40,b=20))

    # MACD
    fig_macd = go.Figure()
    if "macd" in data and "macd_signal" in data:
        fig_macd.add_trace(go.Scatter(x=time, y=data["macd"], mode="lines", name="MACD"))
        fig_macd.add_trace(go.Scatter(x=time, y=data["macd_signal"], mode="lines", name="Signal"))
    fig_macd.update_layout(title="MACD / Signal", margin=dict(l=20,r=20,t=40,b=20))

    # Table (last 200)
    # pick a small set of columns if available
    cols = [c for c in ["time","pair","open","high","low","close","volume","rsi14","macd","macd_signal"] if c in data]
    tail_len = min(200, len(time))
    rows = []
    for i in range(len(time)-tail_len, len(time)):
        row = {c: (data[c][i].isoformat() if c=="time" and data[c][i] else data[c][i]) for c in cols}
        rows.append(row)
    table = dash_table.DataTable(columns=[{"name":c, "id":c} for c in cols], data=rows, page_size=20, style_table={"overflowX":"auto"})

    foot = f"Rows: {len(time):,} • Range: {time[0]} → {time[-1]}"
    return kpis, fig_price, fig_ma, fig_macd, table, foot

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", "10000"))
    app.run_server(host="0.0.0.0", port=port, debug=False)
