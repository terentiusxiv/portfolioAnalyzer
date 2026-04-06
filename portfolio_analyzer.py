#!/usr/bin/env python3
"""
Lightweight Portfolio Analyzer
Primary input: Saxo Bank / Mandatum Trader portfolio report PDF
Enriches with Yahoo Finance for live prices, sector, geography, and correlation.

Usage:
  python portfolio_analyzer.py Portfolio_report.pdf
  python portfolio_analyzer.py                       (auto-detects Portfolio_*.pdf)
"""

import yfinance as yf
import json
import re
import sys
import argparse
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from pathlib import Path
from datetime import datetime, date, timedelta
from collections import defaultdict

ISIN_TICKER_MAP_FILE = "isin_ticker_map.json"
CASH_FLOWS_FILE = "cash_flows.json"
CHARTS_FILE = "portfolio_charts.png"
CORRELATION_CHARTS_FILE = "portfolio_correlation.png"
PICKER_CHARTS_FILE = "portfolio_picker.png"

DEFAULT_ISIN_MAP = {
    "US0079031078": "AMD",
    "FI4000512496": "CANATU.HE",
    "FI0009005870": "KCR.HE",
    "US5128073062": "LRCX",
    "FI4000552526": "MANTA.HE",
    "FI4000198031": "QTCOM.HE",
    "FI0009010912": "REV1V.HE",
    "US92532F1003": "VRTX",
}

ISIN_COUNTRY_PREFIXES = {"US", "FI", "DE", "GB", "FR", "NL", "SE", "DK", "NO", "IE", "CH", "LU", "BE", "JP", "CA"}

COLORS = ["#2563eb", "#16a34a", "#dc2626", "#9333ea", "#ea580c",
          "#0891b2", "#ca8a04", "#e11d48", "#4f46e5", "#059669"]


def parse_num(s):
    return float(s.strip().replace("\u00a0", "").replace(" ", "").replace("%", "").replace(",", "."))


def parse_portfolio_pdf(pdf_path):
    try:
        import pdfplumber
    except ImportError:
        print("pdfplumber not installed. Run: pip install pdfplumber")
        sys.exit(1)

    print(f"Parsing {pdf_path}...")

    with pdfplumber.open(pdf_path) as pdf:
        full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)

    lines = full_text.split("\n")

    # Build line-level ISIN index
    isin_line_map = {}
    for i, line in enumerate(lines):
        for m in re.finditer(r"([A-Z]{2}[A-Z0-9]{10})", line):
            code = m.group(1)
            if code[:2] in ISIN_COUNTRY_PREFIXES:
                isin_line_map[i] = code

    # Find data lines: start with EUR/USD, then integer quantity, then numbers
    data_lines = []
    for i, line in enumerate(lines):
        if re.match(r"^(EUR|USD)\s+\d+\s+[\d,]+\s+[\d,]+\s+[\d,]+", line):
            data_lines.append((i, line))

    # Match each data line to nearest ISIN within 3 lines
    positions = []
    used_isins = set()

    for dl_idx, dl_text in data_lines:
        best_isin, best_dist = None, 999
        for il_idx, isin in isin_line_map.items():
            dist = abs(il_idx - dl_idx)
            if dist < best_dist and dist <= 3 and isin not in used_isins:
                best_dist = dist
                best_isin = isin

        if not best_isin:
            continue
        used_isins.add(best_isin)

        parts = dl_text.split()
        currency = parts[0]
        quantity = int(parts[1])
        nums = re.findall(r"(-?[\d\s\u00a0]+,[\d]+)", dl_text)

        if len(nums) < 6:
            continue

        open_price = parse_num(nums[1])
        current_price = parse_num(nums[2])
        pnl = parse_num(nums[4])
        market_val = parse_num(nums[5])

        # Instrument name from line above
        name_line = lines[dl_idx - 1] if dl_idx > 0 else ""
        name = re.sub(r"\(ISIN:.*", "", name_line).strip()

        positions.append({
            "instrument": name, "isin": best_isin, "currency": currency,
            "quantity": quantity, "open_price": open_price, "current_price": current_price,
            "pnl_eur": pnl, "market_value_eur": market_val,
        })

    # Cash
    cash_eur = 0.0
    cash_match = re.search(r"Allaccounts\s+EUR\s+([\d\s\u00a0]+,\d{2})", full_text)
    if not cash_match:
        cash_match = re.search(r"All accounts\s+EUR\s+([\d\s\u00a0]+,\d{2})", full_text)
    if cash_match:
        cash_eur = parse_num(cash_match.group(1))

    equity_total = sum(p["market_value_eur"] for p in positions)
    account_value = equity_total + cash_eur

    print(f"   Extracted {len(positions)} stock positions")
    print(f"   Cash: EUR {cash_eur:,.2f}")
    print(f"   Account value: EUR {account_value:,.2f}\n")

    return {"positions": positions, "cash_eur": cash_eur, "account_value_eur": account_value}


def load_isin_map():
    if Path(ISIN_TICKER_MAP_FILE).exists():
        with open(ISIN_TICKER_MAP_FILE) as f:
            return json.load(f)
    return dict(DEFAULT_ISIN_MAP)


def save_isin_map(mapping):
    with open(ISIN_TICKER_MAP_FILE, "w") as f:
        json.dump(mapping, f, indent=2)


def resolve_tickers(positions, isin_map):
    updated = dict(isin_map)
    unmapped = [(p["instrument"], p["isin"]) for p in positions if p["isin"] not in updated]
    if unmapped:
        print("--- Unmapped ISINs ---")
        print("Enter Yahoo Finance tickers (.HE for Helsinki).\n")
        for name, isin in unmapped:
            ticker = input(f"  {name} ({isin}) -> ").strip()
            if ticker:
                updated[isin] = ticker
        save_isin_map(updated)
        print(f"\n   Saved to {ISIN_TICKER_MAP_FILE}\n")
    elif not Path(ISIN_TICKER_MAP_FILE).exists():
        save_isin_map(updated)
    return updated


def enrich_positions(positions, isin_map):
    print("Fetching data from Yahoo Finance...")
    enriched = []
    for pos in positions:
        ticker = isin_map.get(pos["isin"])
        if not ticker:
            continue
        try:
            info = yf.Ticker(ticker).info
            enriched.append({
                **pos, "ticker": ticker,
                "sector": info.get("sector", "Unknown"),
                "country": info.get("country", "Unknown"),
                "industry": info.get("industry", "Unknown"),
                "full_name": info.get("longName") or info.get("shortName", pos["instrument"]),
            })
        except Exception as e:
            print(f"   Warning: Failed {ticker}: {e}")
            enriched.append({
                **pos, "ticker": ticker,
                "sector": "Unknown", "country": "Unknown",
                "industry": "Unknown", "full_name": pos["instrument"],
            })
    print(f"   Enriched {len(enriched)} positions\n")
    return enriched


def analyze_allocation(enriched, cash_eur):
    total_equity = sum(p["market_value_eur"] for p in enriched)
    total_portfolio = total_equity + cash_eur
    sector_alloc = defaultdict(float)
    geo_alloc = defaultdict(float)
    for p in enriched:
        sector_alloc[p["sector"]] += p["market_value_eur"]
        geo_alloc[p["country"]] += p["market_value_eur"]
    return {
        "total_portfolio_eur": total_portfolio, "total_equity_eur": total_equity,
        "cash_eur": cash_eur,
        "cash_pct": cash_eur / total_portfolio * 100 if total_portfolio else 0,
        "sector": {k: {"value_eur": v, "pct": v / total_equity * 100}
                   for k, v in sorted(sector_alloc.items(), key=lambda x: -x[1])},
        "geography": {k: {"value_eur": v, "pct": v / total_equity * 100}
                      for k, v in sorted(geo_alloc.items(), key=lambda x: -x[1])},
    }


def calculate_xirr(cash_flows_file, current_value):
    if not Path(cash_flows_file).exists():
        return None
    try:
        from scipy.optimize import brentq
    except ImportError:
        return None
    with open(cash_flows_file) as f:
        flows = json.load(f)
    if not flows:
        return None
    entries = [(datetime.strptime(cf["date"], "%Y-%m-%d").date(), cf["amount"]) for cf in flows]
    entries.append((date.today(), -current_value))
    d0 = entries[0][0]
    years = [(d - d0).days / 365.25 for d, _ in entries]
    amounts = [a for _, a in entries]
    def npv(r):
        return sum(a / (1 + r) ** t for a, t in zip(amounts, years))
    try:
        return brentq(npv, -0.99, 10.0)
    except:
        return None


def compute_correlation_matrix(enriched):
    tickers = [p["ticker"] for p in enriched]
    if len(tickers) < 2:
        return None
    end_date = date.today()
    start_date = end_date - timedelta(days=365)
    print("Fetching trailing 12-month price history...")
    try:
        prices = yf.download(
            tickers, start=start_date.isoformat(), end=end_date.isoformat(),
            auto_adjust=True, progress=False
        )["Close"]
    except Exception as e:
        print(f"   Failed to fetch price history: {e}")
        return None
    if prices.empty:
        return None
    if isinstance(prices, pd.Series):
        prices = prices.to_frame(name=tickers[0])
    valid_cols = [c for c in prices.columns if prices[c].dropna().shape[0] >= 60]
    dropped = set(prices.columns) - set(valid_cols)
    if dropped:
        print(f"   Insufficient data for: {', '.join(str(d) for d in dropped)}")
    prices = prices[valid_cols]
    if prices.shape[1] < 2:
        return None
    returns = np.log(prices / prices.shift(1)).dropna()
    corr_matrix = returns.corr()
    cov_matrix = returns.cov() * 252
    ann_vol = returns.std() * np.sqrt(252)
    equity_total = sum(p["market_value_eur"] for p in enriched)
    ticker_to_weight = {p["ticker"]: p["market_value_eur"] / equity_total
                        for p in enriched if p["ticker"] in valid_cols}
    ordered_tickers = list(cov_matrix.columns)
    w = np.array([ticker_to_weight.get(t, 0) for t in ordered_tickers])
    w = w / w.sum()
    port_var = w @ cov_matrix.values @ w
    port_vol = np.sqrt(port_var)
    mcr = (cov_matrix.values @ w) / port_vol
    ctr = w * mcr
    pct_ctr = ctr / port_vol * 100
    trading_days = returns.shape[0]
    print(f"   Computed from {trading_days} trading days")
    print(f"   Annualized portfolio volatility: {port_vol * 100:.1f}%\n")
    return {
        "corr_matrix": corr_matrix, "cov_matrix": cov_matrix, "ann_vol": ann_vol,
        "returns": returns, "tickers": ordered_tickers, "weights": w,
        "port_vol": port_vol, "pct_ctr": pct_ctr, "trading_days": trading_days,
    }


def print_report(enriched, allocation, xirr):
    print("=" * 70)
    print("  PORTFOLIO ANALYSIS REPORT")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 70)
    print(f"\n  Total portfolio:  EUR {allocation['total_portfolio_eur']:>12,.2f}")
    print(f"  Equity positions: EUR {allocation['total_equity_eur']:>12,.2f}")
    print(f"  Cash:             EUR {allocation['cash_eur']:>12,.2f}  ({allocation['cash_pct']:.1f}%)")
    if xirr is not None:
        print(f"  XIRR:              {xirr * 100:>11.2f}%")
    equity_total = allocation["total_equity_eur"]
    print(f"\n  {'Ticker':<10} {'Name':<25} {'Value EUR':>10} {'Weight':>8} {'P/L EUR':>10}")
    print(f"  {'-'*10} {'-'*25} {'-'*10} {'-'*8} {'-'*10}")
    for p in sorted(enriched, key=lambda x: -x["market_value_eur"]):
        weight = p["market_value_eur"] / equity_total * 100 if equity_total else 0
        name = p["full_name"][:25]
        print(f"  {p['ticker']:<10} {name:<25} {p['market_value_eur']:>10,.0f} {weight:>7.1f}% {p['pnl_eur']:>+10,.0f}")
    print(f"\n  SECTOR ALLOCATION")
    for sector, data in allocation["sector"].items():
        bar = "#" * int(data["pct"] / 2)
        print(f"  {sector:<28} {data['pct']:>5.1f}%  {bar}")
    print(f"\n  GEOGRAPHY ALLOCATION")
    for country, data in allocation["geography"].items():
        bar = "#" * int(data["pct"] / 2)
        print(f"  {country:<28} {data['pct']:>5.1f}%  {bar}")
    print(f"\n{'=' * 70}\n")


def print_correlation_report(corr_data):
    corr = corr_data["corr_matrix"]
    vol = corr_data["ann_vol"]
    tickers = corr_data["tickers"]
    w = corr_data["weights"]
    pct_ctr = corr_data["pct_ctr"]
    print("  CORRELATION MATRIX (trailing 12m daily log returns)")
    print("-" * 70)
    header = "            " + "".join(f"{t:>10}" for t in tickers)
    print(header)
    for i, t in enumerate(tickers):
        row = f"  {t:<10}" + "".join(f"{corr.iloc[i, j]:>10.2f}" for j in range(len(tickers)))
        print(row)
    print(f"\n  RISK DECOMPOSITION")
    print(f"  {'Ticker':<10} {'Weight':>8} {'Ann.Vol':>10} {'Risk Contr.':>12}")
    print(f"  {'-'*10} {'-'*8} {'-'*10} {'-'*12}")
    for i, t in enumerate(tickers):
        print(f"  {t:<10} {w[i]*100:>7.1f}% {vol.iloc[i]*100:>9.1f}% {pct_ctr[i]:>11.1f}%")
    print(f"\n  Portfolio volatility: {corr_data['port_vol'] * 100:.1f}%")
    print(f"  Based on {corr_data['trading_days']} trading days\n")


def generate_charts(enriched, allocation):
    fig = plt.figure(figsize=(16, 12), facecolor="white")
    fig.suptitle(f"Portfolio Analysis  -  {datetime.now().strftime('%Y-%m-%d')}",
                 fontsize=16, fontweight="bold", y=0.97)
    gs = fig.add_gridspec(2, 2, hspace=0.40, wspace=0.30,
                          left=0.07, right=0.95, top=0.90, bottom=0.08)
    ax1 = fig.add_subplot(gs[0, :])
    sorted_pos = sorted(enriched, key=lambda x: x["market_value_eur"])
    equity_total = allocation["total_equity_eur"]
    labels = [p["ticker"] for p in sorted_pos]
    values = [p["market_value_eur"] for p in sorted_pos]
    weights = [v / equity_total * 100 for v in values]
    pnl = [p["pnl_eur"] for p in sorted_pos]
    bar_colors = ["#16a34a" if pl >= 0 else "#dc2626" for pl in pnl]
    bars = ax1.barh(labels, values, color=bar_colors, edgecolor="white", height=0.6)
    for bar, w, v in zip(bars, weights, values):
        ax1.text(bar.get_width() + equity_total * 0.008, bar.get_y() + bar.get_height() / 2,
                 f"EUR {v:,.0f}  ({w:.1f}%)", va="center", fontsize=9, color="#374151")
    ax1.set_title("Holdings by Market Value", fontsize=12, fontweight="bold", loc="left", pad=10)
    ax1.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"EUR {x:,.0f}"))
    ax1.spines[["top", "right"]].set_visible(False)
    ax1.set_xlim(0, max(values) * 1.30)
    from matplotlib.patches import Patch
    ax1.legend(handles=[Patch(color="#16a34a", label="P/L >= 0"), Patch(color="#dc2626", label="P/L < 0")],
               loc="lower right", fontsize=8)

    ax2 = fig.add_subplot(gs[1, 0])
    s_labels = list(allocation["sector"].keys())
    s_values = [allocation["sector"][s]["value_eur"] for s in s_labels]
    s_pcts = [allocation["sector"][s]["pct"] for s in s_labels]
    wedges, _ = ax2.pie(s_values, colors=COLORS[:len(s_labels)], startangle=90,
                        wedgeprops=dict(width=0.45, edgecolor="white", linewidth=2))
    ax2.set_title("Sector Allocation", fontsize=12, fontweight="bold", pad=12)
    ax2.legend(wedges, [f"{s}  ({p:.1f}%)" for s, p in zip(s_labels, s_pcts)],
               loc="center", fontsize=8, frameon=False)

    ax3 = fig.add_subplot(gs[1, 1])
    g_labels = list(allocation["geography"].keys())
    g_values = [allocation["geography"][g]["value_eur"] for g in g_labels]
    g_pcts = [allocation["geography"][g]["pct"] for g in g_labels]
    wedges2, _ = ax3.pie(g_values, colors=COLORS[:len(g_labels)], startangle=90,
                         wedgeprops=dict(width=0.45, edgecolor="white", linewidth=2))
    ax3.set_title("Geography Allocation", fontsize=12, fontweight="bold", pad=12)
    ax3.legend(wedges2, [f"{g}  ({p:.1f}%)" for g, p in zip(g_labels, g_pcts)],
               loc="center", fontsize=8, frameon=False)

    total = allocation["total_portfolio_eur"]
    fig.text(0.5, 0.935,
             f"Total: EUR {total:,.2f}   |   Equity: EUR {equity_total:,.0f}   |   Cash: EUR {allocation['cash_eur']:,.0f} ({allocation['cash_pct']:.1f}%)",
             ha="center", fontsize=10, color="#6b7280")
    plt.savefig(CHARTS_FILE, dpi=150, bbox_inches="tight")
    print(f"   Charts saved to {CHARTS_FILE}")


def generate_correlation_charts(corr_data):
    corr = corr_data["corr_matrix"]
    tickers = corr_data["tickers"]
    w = corr_data["weights"]
    pct_ctr = corr_data["pct_ctr"]
    vol = corr_data["ann_vol"]
    n = len(tickers)
    fig, axes = plt.subplots(1, 2, figsize=(16, 7), facecolor="white",
                              gridspec_kw={"width_ratios": [1.2, 1], "wspace": 0.35})
    fig.suptitle(f"Correlation & Risk  -  trailing 12m  ({corr_data['trading_days']} days)",
                 fontsize=14, fontweight="bold", y=0.97)
    ax1 = axes[0]
    im = ax1.imshow(corr.values, cmap=plt.cm.RdYlGn_r, vmin=-1, vmax=1, aspect="equal")
    ax1.set_xticks(range(n))
    ax1.set_yticks(range(n))
    ax1.set_xticklabels(tickers, fontsize=9, rotation=45, ha="right")
    ax1.set_yticklabels(tickers, fontsize=9)
    for i in range(n):
        for j in range(n):
            val = corr.iloc[i, j]
            color = "white" if abs(val) > 0.6 else "black"
            ax1.text(j, i, f"{val:.2f}", ha="center", va="center",
                     fontsize=9, fontweight="bold" if i == j else "normal", color=color)
    fig.colorbar(im, ax=ax1, fraction=0.046, pad=0.04).set_label("Correlation", fontsize=9)
    ax1.set_title("Pairwise Correlation", fontsize=12, fontweight="bold", pad=12)

    ax2 = axes[1]
    x = np.arange(n)
    bw = 0.35
    ax2.bar(x - bw / 2, w * 100, bw, label="Portfolio Weight", color="#2563eb", alpha=0.8)
    risk_bars = ax2.bar(x + bw / 2, pct_ctr, bw, label="Risk Contribution", color="#dc2626", alpha=0.8)
    ax2.set_xticks(x)
    ax2.set_xticklabels(tickers, fontsize=9, rotation=45, ha="right")
    ax2.set_ylabel("Percentage (%)", fontsize=9)
    ax2.set_title("Weight vs Risk Contribution", fontsize=12, fontweight="bold", pad=12)
    ax2.legend(fontsize=9, loc="upper right")
    ax2.spines[["top", "right"]].set_visible(False)
    for bar, v in zip(risk_bars, vol):
        ax2.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.8,
                 f"vol={v*100:.0f}%", ha="center", fontsize=7, color="#6b7280")
    plt.savefig(CORRELATION_CHARTS_FILE, dpi=150, bbox_inches="tight")
    print(f"   Correlation charts saved to {CORRELATION_CHARTS_FILE}")


def generate_picker_chart(results, corr_data):
    existing_tickers = corr_data["tickers"]
    n_cands = len(results)
    fig_h = max(5, n_cands * 1.0 + 3)
    fig, axes = plt.subplots(1, 2, figsize=(16, fig_h), facecolor="white",
                             gridspec_kw={"width_ratios": [1, 1.4], "wspace": 0.45})
    fig.suptitle("Stock Picker  —  Diversification Analysis",
                 fontsize=14, fontweight="bold", y=0.98)

    # Left: average correlation per candidate
    ax1 = axes[0]
    cand_labels = [r["ticker"] for r in results]
    avg_corrs = [r["avg_corr"] for r in results]
    bar_colors = ["#16a34a" if c < 0.3 else ("#ca8a04" if c < 0.6 else "#dc2626")
                  for c in avg_corrs]
    y = np.arange(n_cands)
    bars = ax1.barh(y, avg_corrs, color=bar_colors, edgecolor="white", height=0.5, alpha=0.88)
    ax1.set_yticks(y)
    ax1.set_yticklabels(cand_labels, fontsize=10)
    ax1.set_xlim(0, 1.0)
    ax1.set_xlabel("Average Correlation with Portfolio", fontsize=9)
    ax1.set_title("Avg Correlation  (lower = better)", fontsize=11, fontweight="bold", pad=10)
    ax1.axvline(0.3, color="#16a34a", linestyle="--", linewidth=1, alpha=0.7, label="Good fit  (<0.30)")
    ax1.axvline(0.6, color="#dc2626", linestyle="--", linewidth=1, alpha=0.7, label="High overlap  (>0.60)")
    ax1.spines[["top", "right"]].set_visible(False)
    ax1.legend(fontsize=8, loc="lower right")
    for bar, r in zip(bars, results):
        label = f"{r['avg_corr']:.2f}   vol {r['vol_delta']:+.1f}%"
        ax1.text(min(bar.get_width() + 0.02, 0.98), bar.get_y() + bar.get_height() / 2,
                 label, va="center", fontsize=8, color="#374151")

    # Right: per-holding correlation heatmap  (candidates × existing)
    ax2 = axes[1]
    matrix = np.array([[r["pair_corrs"].get(t, np.nan) for t in existing_tickers]
                       for r in results])
    im = ax2.imshow(matrix, cmap=plt.cm.RdYlGn_r, vmin=-1, vmax=1, aspect="auto")
    ax2.set_xticks(range(len(existing_tickers)))
    ax2.set_yticks(range(n_cands))
    ax2.set_xticklabels(existing_tickers, fontsize=9, rotation=45, ha="right")
    ax2.set_yticklabels(cand_labels, fontsize=9)
    for i in range(n_cands):
        for j in range(len(existing_tickers)):
            val = matrix[i, j]
            if not np.isnan(val):
                txt_color = "white" if abs(val) > 0.6 else "black"
                ax2.text(j, i, f"{val:.2f}", ha="center", va="center",
                         fontsize=9, color=txt_color)
    fig.colorbar(im, ax=ax2, fraction=0.046, pad=0.04).set_label("Correlation", fontsize=9)
    ax2.set_title("Candidate vs Each Holding", fontsize=11, fontweight="bold", pad=10)

    plt.savefig(PICKER_CHARTS_FILE, dpi=150, bbox_inches="tight")
    print(f"   Picker chart saved to {PICKER_CHARTS_FILE}")


def pick_stocks(candidates, corr_data, enriched, test_weight=0.05):
    """Compare candidate tickers against existing portfolio for diversification."""
    existing_returns = corr_data["returns"]
    end_date = date.today()
    start_date = end_date - timedelta(days=365)

    results = []
    for ticker in candidates:
        print(f"Analyzing candidate: {ticker}...")
        try:
            raw = yf.download(
                ticker, start=start_date.isoformat(), end=end_date.isoformat(),
                auto_adjust=True, progress=False,
            )["Close"]
            prices = raw.squeeze() if isinstance(raw, pd.DataFrame) else raw
            if prices.dropna().shape[0] < 60:
                print(f"   Insufficient price history for {ticker}, skipping.")
                continue

            cand_returns = np.log(prices / prices.shift(1)).dropna()
            cand_returns.name = ticker

            combined = existing_returns.join(cand_returns, how="inner")
            if combined.shape[0] < 60:
                print(f"   Not enough overlapping trading days for {ticker}, skipping.")
                continue

            existing_cols = [c for c in combined.columns if c != ticker]
            pair_corrs = {t: combined[ticker].corr(combined[t]) for t in existing_cols}
            avg_corr = float(np.mean(list(pair_corrs.values())))

            # Simulate portfolio vol with candidate at test_weight
            existing_tickers = corr_data["tickers"]
            valid_existing = [t for t in existing_tickers if t in combined.columns]
            sim_returns = combined[valid_existing + [ticker]]
            sim_cov = sim_returns.cov() * 252

            total_w = sum(corr_data["weights"][existing_tickers.index(t)] for t in valid_existing)
            sim_w = np.array(
                [corr_data["weights"][existing_tickers.index(t)] / total_w * (1 - test_weight)
                 for t in valid_existing]
                + [test_weight]
            )
            sim_vol = float(np.sqrt(sim_w @ sim_cov.values @ sim_w))
            vol_delta = (sim_vol - corr_data["port_vol"]) * 100

            try:
                info = yf.Ticker(ticker).info
                name = info.get("longName") or info.get("shortName", ticker)
                sector = info.get("sector", "Unknown")
            except Exception:
                name, sector = ticker, "Unknown"

            results.append({
                "ticker": ticker, "name": name, "sector": sector,
                "avg_corr": avg_corr, "pair_corrs": pair_corrs,
                "sim_vol": sim_vol, "vol_delta": vol_delta,
            })
        except Exception as e:
            print(f"   Error analyzing {ticker}: {e}")

    if not results:
        print("No valid candidates to compare.")
        return

    results.sort(key=lambda x: x["avg_corr"])
    print_stock_pick_report(results, corr_data, test_weight)


def print_stock_pick_report(results, corr_data, test_weight):
    print("=" * 70)
    print("  STOCK PICKER  —  Diversification Analysis")
    print(f"  Simulated at {test_weight*100:.0f}% allocation "
          f"(existing holdings scaled to {(1-test_weight)*100:.0f}%)")
    print("=" * 70)
    print(f"  Current portfolio volatility: {corr_data['port_vol']*100:.1f}%\n")
    print(f"  {'Rank':<5} {'Ticker':<10} {'Sector':<22} {'Avg Corr':>9} {'Sim Vol':>8} {'Vol Δ':>8}  Verdict")
    print(f"  {'-'*5} {'-'*10} {'-'*22} {'-'*9} {'-'*8} {'-'*8}  {'-'*12}")

    for rank, r in enumerate(results, 1):
        if r["avg_corr"] < 0.3:
            verdict = "GOOD FIT"
        elif r["avg_corr"] < 0.6:
            verdict = "NEUTRAL"
        else:
            verdict = "HIGH OVERLAP"
        delta_str = f"{r['vol_delta']:+.1f}%"
        print(f"  {rank:<5} {r['ticker']:<10} {r['sector'][:22]:<22} "
              f"{r['avg_corr']:>9.2f} {r['sim_vol']*100:>7.1f}% {delta_str:>8}  {verdict}")

    print()
    for r in results:
        print(f"  {r['ticker']}  ({r['name'][:50]})")
        print(f"  {'Holding':<12} {'Correlation':>12}")
        for t, c in sorted(r["pair_corrs"].items(), key=lambda x: x[1]):
            print(f"  {t:<12} {c:>12.2f}")
        print()
    print("=" * 70 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Lightweight Portfolio Analyzer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Examples:\n"
               "  python portfolio_analyzer.py Portfolio_report.pdf\n"
               "  python portfolio_analyzer.py --pick NVDA TSM ASML\n"
               "  python portfolio_analyzer.py Portfolio_report.pdf --pick NVDA TSM",
    )
    parser.add_argument("pdf", nargs="?", help="Portfolio PDF report (auto-detected if omitted)")
    parser.add_argument("--pick", nargs="+", metavar="TICKER",
                        help="Candidate ticker(s) to evaluate for diversification")
    parser.add_argument("--test-weight", type=float, default=0.05, metavar="W",
                        help="Simulated allocation for each candidate (default: 0.05)")
    args = parser.parse_args()

    if args.pdf:
        pdf_path = args.pdf
    else:
        pdfs = list(Path(".").glob("Portfolio_*.pdf"))
        if pdfs:
            pdf_path = str(pdfs[0])
            print(f"Auto-detected: {pdf_path}\n")
        else:
            parser.print_help()
            sys.exit(1)

    if not Path(pdf_path).exists():
        print(f"File not found: {pdf_path}")
        sys.exit(1)

    holdings = parse_portfolio_pdf(pdf_path)
    isin_map = load_isin_map()
    isin_map = resolve_tickers(holdings["positions"], isin_map)
    enriched = enrich_positions(holdings["positions"], isin_map)

    if not enriched:
        print("No enriched positions. Check isin_ticker_map.json.")
        sys.exit(1)

    allocation = analyze_allocation(enriched, holdings["cash_eur"])
    xirr = calculate_xirr(CASH_FLOWS_FILE, holdings["account_value_eur"])

    corr_data = compute_correlation_matrix(enriched)

    print_report(enriched, allocation, xirr)
    if corr_data:
        print_correlation_report(corr_data)

    generate_charts(enriched, allocation)
    if corr_data:
        generate_correlation_charts(corr_data)

    if args.pick:
        if not corr_data:
            print("Cannot run stock picker: correlation matrix unavailable "
                  "(need >= 2 positions with sufficient history).")
            sys.exit(1)
        pick_stocks(args.pick, corr_data, enriched, test_weight=args.test_weight)

    plt.show()


if __name__ == "__main__":
    main()
