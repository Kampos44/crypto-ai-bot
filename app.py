import os
import psycopg
from dash import Dash, dcc, html, Input, Output, callback, no_update, dash_table
import plotly.graph_objects as go

# =========================
# Config
# =========================
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is missing. Set it in Render → Environment.")

# =========================
# DB helpers
# =========================
def fetch_pairs():
    """Return list of distinct trading pairs."""
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT pair FROM cryptoprices ORDER BY pair;")
            return [r[0] for r in cur.fetchall()]

def fetch_data(pair: str, days: int | None):
    """
    Fetch OHLC + indicators for a pair. If days is None → fetch all history.
    Handles quoted \"MA75\" vs unquoted ma75.
    Returns dict-of-lists for easy Dash plotting (no pandas required).
    """
    # Base SELECT (we parametrize the MA75 expression to survive quoted column names)
    base = """
        SELECT time, pair, open, high, low, close, volume,
               avg_volume_20, rsi14, macd, macd_signal,
               bb_upper, bb_lower, bb_basis,
               ma50, ma100, ma200, sma10, sma50,
               vwma10, vwma20, vwma50,
               {ma75_expr}
        FROM cryptoprices
        WHERE pair = %s
        {time_clause}
        ORDER BY time ASC
    """
    time_clause = "" if days is None else "AND time >= NOW() - make_interval(days => %s)"

    def run_query(ma75_expr):
        sql = base.format(ma75_expr=ma75_expr, time_clause=time_clause)
        params = (pair,) if days is None else (pair, days)
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
                cols = [d.name for d in cur.description]
        data = {c: [] for c in cols}
        for r in rows:
            for c, v in zip(cols, r):
                data[c].append(v)
        return data

    # Try unquoted ma75 first; if it errors or missing, retry with quoted "MA75" AS ma75
    try:
        data = run_query("ma75")
        if "ma75" not in data:
            # When SELECT fails to alias we won't hit this, but keep a guard anyway
            raise KeyError("ma75 not present")
        return data
    except Exception:
        return run_query('"MA75" AS ma75')

# =========================
# App
# =========================
app = Dash(__name__)
server = app.server  # for gunicorn

try:
    PAIRS = fetch_pairs()
except Exception:
    PAIRS = []

default_pair = "BTCUSDT" if "BTCUSDT" in PAIRS else (PAIRS[0] if PAIRS else None)

app.layout = html.Div(
    style={"maxWidth": "1100px", "margin": "0 auto", "padding": "18px"},
    children=[
        html.H2("📈 Crypto Dashboard (Neon → Dash)"),
        html.Div(
            style={"display": "flex", "gap": "12px", "flexWrap": "wrap", "alignItems": "center"},
            children=[
                html.Div([
                    html.Label("Pair"),
                    dcc.Dropdown(
                        id="pair",
                        options=[{"label": p, "value": p} for p in PAIRS],
                        value=default_pair,
                        clearable=False,
                        style={"minWidth": "240px"},
                    ),
                ]),
                html.Div(style={"minWidth": "340px", "flex": "1"}, children=[
                    html.Label("Days of history"),
                    dcc.Slider(
                        id="days",
                        min=1, max=365, step=1, value=90,
                        marks={1: "1", 7: "7", 30: "30", 90: "90", 180: "180", 365: "365"},
                        tooltip={"placement": "bottom", "always_visible": False},
                    ),
                ]),
                dcc.Checklist(
                    id="all-data",
                    options=[{"label": " Show all history", "value": "ALL"}],
                    value=[],
                    style={"whiteSpace": "nowrap"},
                ),
            ],
        ),
        html.Hr(),
        html.Div(id="kpis"),
        dcc.Graph(id="price_chart"),
        dcc.Graph(id="ma_chart"),
        dcc.Graph(id="macd_chart"),
        html.H3("Raw data (latest 200)"),
        html.Div(id="table"),
        html.Div(id="footnote", style={"color": "#666", "marginTop": "8px"}),
    ],
)

# =========================
# Callback
# =========================
@callback(
    Output("kpis", "children"),
    Output("price_chart", "figure"),
    Output("ma_chart", "figure"),
    Output("macd_chart", "figure"),
    Output("table", "children"),
    Output("footnote", "children"),
    Input("pair", "value"),
    Input("days", "value"),
    Input("all-data", "value"),
)
def update(pair, days, all_data_values):
    if not pair:
        return (
            html.Div("No pairs found in table `cryptoprices`."),
            no_update, no_update, no_update, no_update, "",
        )

    days_param = None if ("ALL" in (all_data_values or [])) else int(days)

    try:
        data = fetch_data(pair, days_param)
    except Exception as e:
        return (html.Div(f"Query error: {e}"), no_update, no_update, no_update, no_update, "")

    if not data.get("time"):
        return (html.Div("No rows for the selected filters."), no_update, no_update, no_update, no_update, "")

    time = data["time"]
    close = data.get("close", [])
    volume = data.get("volume", [])
    rsi14 = data.get("rsi14", [])
    macd = data.get("macd", [])
    macd_signal = data.get("macd_signal", [])

    # ---- KPIs
    def fmt(x, d=6):
        try:
            return f"{float(x):.{d}f}"
        except Exception:
            return "—"

    latest = -1
    kpis = html.Div(
        style={"display": "grid", "gridTemplateColumns": "repeat(4, 1fr)", "gap": "12px"},
        children=[
            html.Div([html.Div("Close", style={"color": "#666"}), html.H3(fmt(close[latest], 6) if close else "—")]),
            html.Div([html.Div("RSI(14)", style={"color": "#666"}), html.H3(fmt(rsi14[latest], 2) if rsi14 else "—")]),
            html.Div([html.Div("MACD", style={"color": "#666"}), html.H3(fmt(macd[latest], 6) if macd else "—")]),
            html.Div([html.Div("Volume", style={"color": "#666"}), html.H3(fmt(volume[latest], 0) if volume else "—")]),
        ],
    )

    # ---- Price chart
    fig_price = go.Figure()
    fig_price.add_trace(go.Scatter(x=time, y=close, mode="lines", name="Close"))
    fig_price.update_layout(title=f"Price • {pair}", margin=dict(l=20, r=20, t=40, b=20))

    # ---- MA / Bands chart
    fig_ma = go.Figure()
    def maybe_add(name, label=None):
        series = data.get(name)
        if series and any(v is not None for v in series):
            fig_ma.add_trace(go.Scatter(x=time, y=series, mode="lines", name=(label or name).upper()))

    for col in ["sma10", "sma50", "ma50", "ma75", "ma100", "ma200", "bb_upper", "bb_lower", "bb_basis"]:
        maybe_add(col)
    fig_ma.update_layout(title="Moving Averages / Bands", margin=dict(l=20, r=20, t=40, b=20))

    # ---- MACD chart
    fig_macd = go.Figure()
    if macd and macd_signal:
        fig_macd.add_trace(go.Scatter(x=time, y=macd, mode="lines", name="MACD"))
        fig_macd.add_trace(go.Scatter(x=time, y=macd_signal, mode="lines", name="Signal"))
    fig_macd.update_layout(title="MACD / Signal", margin=dict(l=20, r=20, t=40, b=20))

    # ---- Table (last 200 rows)
    cols = [c for c in ["time", "pair", "open", "high", "low", "close", "volume", "rsi14", "macd", "macd_signal"] if c in data]
    tail_len = min(200, len(time))
    rows = []
    start = len(time) - tail_len
    for i in range(start, len(time)):
        row = {}
        for c in cols:
            v = data[c][i]
            row[c] = v.isoformat(sep=" ") if c == "time" and v else v
        rows.append(row)

    table = dash_table.DataTable(
        columns=[{"name": c, "id": c} for c in cols],
        data=rows,
        page_size=20,
        style_table={"overflowX": "auto"},
        sort_action="native",
        filter_action="native",
    )

    foot = f"Rows: {len(time):,} • Range: {time[0]} → {time[-1]}"

    return kpis, fig_price, fig_ma, fig_macd, table, foot

# =========================
# Entry point
# =========================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run_server(host="0.0.0.0", port=port, debug=False)
