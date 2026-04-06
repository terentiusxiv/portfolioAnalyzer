# Portfolio Analyzer

Parses a Saxo Bank / Mandatum Trader portfolio PDF report, enriches it with live data from Yahoo Finance, and produces allocation analysis, risk metrics, and charts.

## Requirements

```bash
pip install yfinance pdfplumber pandas numpy matplotlib scipy
```

## Usage

```bash
# Basic analysis (auto-detects Portfolio_*.pdf in current folder)
python portfolio_analyzer.py

# Specify a PDF explicitly
python portfolio_analyzer.py Portfolio_report.pdf

# Evaluate one or more stocks for diversification
python portfolio_analyzer.py Portfolio_report.pdf --pick NVDA TSM ASML

# Change the simulated allocation weight for candidates (default: 5%)
python portfolio_analyzer.py Portfolio_report.pdf --pick NVDA --test-weight 0.10
```

## First run

If the script encounters a stock it cannot identify, it will prompt you to enter the Yahoo Finance ticker interactively. Tickers are saved to `isin_ticker_map.json` so you only need to do this once. Use the `.HE` suffix for Helsinki-listed stocks (e.g. `NOKIA.HE`).

## Output

| File | Contents |
|---|---|
| `portfolio_charts.png` | Holdings by value, sector and geography allocation |
| `portfolio_correlation.png` | Pairwise correlation heatmap, weight vs risk contribution |
| `portfolio_picker.png` | Candidate diversification scores and per-holding correlations |

## Optional: XIRR

To calculate your annualised return, create a `cash_flows.json` file listing all deposits and withdrawals:

```json
[
  {"date": "2022-01-15", "amount": 10000},
  {"date": "2023-06-01", "amount": 5000}
]
```

Use positive amounts for deposits and negative for withdrawals.
