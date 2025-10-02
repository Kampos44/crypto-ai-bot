import os
from datetime import datetime, timedelta

import pandas as pd
import psycopg
from dash import Dash, dcc, html, Input, Output, callback, no_update
import plotly.express as px

# -----------------------
# Config / connection
# -----------------------
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL env var is missing. Set it in Render → Environment.")

def run_sql(sql: str, params=None) -> pd.DataFrame:
    with psycopg.connect(DATABASE_URL) as conn:
        return pd.read_sql(sql, conn, params=params)

def get_pairs() -> list[str]:
    df = run_sql("SELECT DISTINCT pair FROM cryptoprices ORDER BY pair")
    return df["pair"].tolist()

def load_data(pair: str, days: int) -> pd.DataFrame:
    base_sql = """
        SELECT
            time, pair, open, high, low, close, volume,
            avg_volume_20, rsi14, macd, macd_signal,
            bb_upper, bb_lower, bb_basis,
            ma50, ma100, ma200, sma10, sma50,
            vwma10, vwma20, vwma50,
            {ma75_expr}
        FROM cryptoprices
        WHERE pair = %s
          AND time >= NOW() - INTERVAL '%s days'
        ORDER BY time ASC
    """
    # Handle possible quoted "MA75" column
    try:
        df = run_sql(base_sql.format(ma75_expr="ma75"), (pair, days))
        if "ma75" not in df.columns:
            raise KeyError("ma75 missing; try quoted")
        return df
    except Exception:
        return run_sql(base_sql.format(ma75_expr='"MA75" AS ma75'), (pair, days))

# -----------------------
# App
# -----------------------
app = Dash(__name__)
server = app.server  # for gunicorn

# Preload pairs at startup (fast query)
try:
    PAIRS = get_pairs()
except Exception as e:
    PAIRS = []
    # You could log e here if desired

default_pair = "BTCUSDT" if "BTCUSDT" in PAIRS else (PAIRS[0] if PAIRS else None)

app.layout = html.Div(
    style={"maxWidth": "1200px", "margin": "0 auto", "padding": "20px"},
    children=[
        html.H2("📈 Crypto Dashboard (Neon → Dash)"),
        html.Div(
            style={"display": "flex", "gap": "12px", "alignItems": "center", "flexWrap": "wrap"},
            children=[
                html.Div([
                    html.Label("Pair"),
                    dcc.Dropdown(
                        id="pair",
                        options=[{"label": p, "value": p} for p in PAIRS],
                        value=default_pair,
                        clearable=False,
                        style={"minWidth": "220px"},
                    ),
                ]),
                html.Div([
                    html.Label("Days of history"),
                    dcc.Slider(id="days", min=7, max=365, step=1, value=90,
                               marks={7:"7", 30:"30", 90:"90", 180:"180", 365:"365"},
                               tooltip={"placement": "bottom", "always_visible": False},
                               ),
                ], style={"minWidth": "320px", "flex": "1"})
            ]
        ),
        html.Hr(),
        html.Div(id="kpis"),
        dcc.Graph(id="price_chart"),
        dcc.Graph(id="ma_chart"),
        dcc.Graph(id="macd_chart"),
        html.H3("Raw data (latest 200)"),
        html.Div(id="table"),
        html.Div(id="footnote", style={"color": "#666", "marginTop": "8px"}),
    ]
)

@callback(
    Output("kpis", "children"),
    Output("price_chart", "figure"),
    Output("ma_chart", "figure"),
    Output("macd_chart", "figure"),
    Output("table", "children"),
    Output("footnote", "children"),
    Input("pair", "value"),
    Input("days", "value"),
)
def update(pair, days):
    if not pair:
        return (html.Div("No pairs found in table `cryptoprices`."), no_update, no_update, no_update, no_update, "")

    try:
        df = load_data(pair, days)
    except Exception as e:
        return (html.Div(f"Query error: {e}"), no_update, no_update, no_update, no_update, "")

    if df.empty:
        return (html.Div("No rows for the chosen filters."), no_update, no_update, no_update, no_update, "")

    # KPIs
    latest = df.iloc[-1]
    def fmt(n, d=6):
        try:
            return f"{float(n):.{d}f}"
        except Exception:
            return "—"

    kpis = html.Div(
        style={"display": "grid", "gridTemplateColumns": "repeat(4,1fr)", "gap": "12px"},
        children=[
            html.Div([html.Div("Close", style={"color":"#666"}), html.H3(fmt(latest.get("close"), 6))], className="card"),
            html.Div([html.Div("RSI(14)", style={"color":"#666"}), html.H3(fmt(latest.get("rsi14"), 2))], className="card"),
            html.Div([html.Div("MACD", style={"color":"#666"}), html.H3(fmt(latest.get("macd"), 6))], className="card"),
            html.Div([html.Div("Volume", style={"color":"#666"}), html.H3(fmt(latest.get("volume"), 0))], className="card"),
        ],
    )

    # Price chart
    fig_price = px.line(df, x="time", y="close", title=f"Price • {pair}")
    fig_price.update_layout(margin=dict(l=20, r=20, t=40, b=20))

    # MA chart (show available MAs/Bands)
    cols = [c for c in ["close","sma10","sma50","ma50","ma75","ma100","ma200","bb_upper","bb_lower","bb_basis"]
            if c in df.columns]
    fig_ma = px.line(df, x="time", y=cols, title="Moving Averages / Bands")
    fig_ma.update_layout(margin=dict(l=20, r=20, t=40, b=20), legend_title_text="Series")

    # MACD chart
    macd_cols = [c for c in ["macd","macd_signal"] if c in df.columns]
    if macd_cols:
        fig_macd = px.line(df, x="time", y=macd_cols, title="MACD / Signal")
    else:
        fig_macd = px.line(pd.DataFrame({"time":[], "value":[]}), x="time", y="value", title="MACD / Signal (missing)")
    fig_macd.update_layout(margin=dict(l=20, r=20, t=40, b=20))

    # Table (last 200 rows)
    tail = df.tail(200).copy()
    tail["time"] = pd.to_datetime(tail["time"]).dt.strftime("%Y-%m-%d %H:%M")
    table = dcc.Markdown(tail.to_markdown(index=False))

    foot = f"Rows: {len(df):,} • Range: {df['time'].min()} → {df['time'].max()}"

    return kpis, fig_price, fig_ma, fig_macd, table, foot

if __name__ == "__main__":
    # Bind to the PORT Render provides
    port = int(os.environ.get("PORT", "10000"))
    app.run_server(host="0.0.0.0", port=port, debug=False)
