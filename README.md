# Portfolio Analyzer

Parses a portfolio report (Saxo Bank / Mandatum Trader PDF, Nordea xlsx or universal CSV), enriches it with live data from Yahoo Finance, and produces allocation analysis, risk metrics, and charts.

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

# Use a custom date range for correlation and price history
python portfolio_analyzer.py Portfolio_report.pdf --start 2023-01-01 --end 2024-01-01

# Custom start date, end defaults to today
python portfolio_analyzer.py Portfolio_report.pdf --start 2022-06-01
```

<img width="2013" height="1037" alt="portfolio_correlation" src="https://github.com/user-attachments/assets/d74c16af-1806-4444-a078-960c63ee9670" />

## Supported input formats

**Saxo Bank / Mandatum Trader PDF** — detected automatically from the file content.

**Nordea xlsx** — export from Nordea Netbank → Portfolio → Download (saves as `Omistukset.xlsx` or similar). The file must contain a `Holdings` sheet in the standard Nordea format. Detected automatically.

**Universal CSV** — any broker whose export can be mapped to these columns:

| Column | Description |
|---|---|
| `instrument` | Company name |
| `isin` | ISIN code |
| `currency` | Position currency (e.g. `EUR`, `USD`) |
| `quantity` | Number of shares |
| `open_price` | Average purchase price |
| `current_price` | Latest price |
| `pnl_eur` | Unrealised P/L in EUR |
| `market_value_eur` | Current market value in EUR |

To include a cash balance, add a row with `isin=CASH` and set `market_value_eur` to the cash amount (all other fields can be `0`).

```csv
instrument,isin,currency,quantity,open_price,current_price,pnl_eur,market_value_eur
Advanced Micro Devices,US0079031078,USD,100,80.00,175.50,9550.00,16092.35
Lam Research,US5128073062,USD,20,650.00,920.00,5400.00,16856.20
Cash,CASH,EUR,0,0,0,0,3500.00
```

## First run

If the script encounters a stock it cannot identify, it queries the [OpenFIGI](https://www.openfigi.com/) API and suggests a Yahoo Finance ticker automatically:

```
  Apple Inc (US0378331005)
  Suggestion: AAPL
  Accept [Enter], override with ticker, or '-' to skip:
```

Press **Enter** to accept the suggestion, type a different ticker to override, or type `-` to skip. Tickers are saved to `isin_ticker_map.json` so you are only prompted once per ISIN. If OpenFIGI returns no result, you are prompted to type the ticker manually (e.g. `NOKIA.HE` for Helsinki-listed stocks).

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

---

*Developed with assistance from [Claude](https://claude.ai)*
