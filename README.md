# XAUUSD Hourly High/Low Logger

Logs the **past hour** (hourly boundaries: 8am, 9am, …) **High** and **Low** for XAUUSD from Interactive Brokers.

## Setup

- **TWS or IB Gateway** running and logged in.
- **API** enabled: Configure → API → Settings → Enable ActiveX and Socket Clients; port **4002** (or set `IB_PORT` in the script).
- **XAUUSD** contract must match your spec (Commodities, USD, SMART; script uses conId `69067924`).

```bash
pip install -r requirements.txt
```

## Run

```bash
python hourly_xauusd_hl.py
```

- Connects to `127.0.0.1:4002`.
- Once per hour, at **1 minute past the hour**, requests the last completed 1-hour bar and logs its **high** and **low**.
- Log line format: `XAUUSD past hour HL | bar_end=... | high=... | low=...`

## Contract (from your spec)

- Underlying: XAUUSD  
- Security Type: Commodities  
- Currency: USD  
- Exchange: SMART  
- Contract ID: 69067924  

If your conId differs, edit `XAUUSD` in `hourly_xauusd_hl.py`.
