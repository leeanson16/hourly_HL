"""
Log past hour (hourly start: 8am, 9am, ...) High and Low for multiple assets from Interactive Brokers.
Runs every hour at 20 seconds past the hour; logs the completed hour's HL per asset.
Schedule window (days + times, HKT) is read from config.yaml.
"""
import time
import logging
import math
from datetime import datetime, timezone, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from ib_insync import IB, Contract, Crypto, Forex, Index

HKT = ZoneInfo("Asia/Hong_Kong")

# config.yaml next to this script
SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = SCRIPT_DIR / "config.yaml"

# TWS/Gateway
IB_PORT = 4002
IB_HOST = "127.0.0.1"
CLIENT_ID = 1

# XAUUSD: Commodities, SMART, conId 69067924
XAUUSD_CMDTY = Contract(conId=69067924, symbol="XAUUSD", secType="CMDTY", exchange="SMART", currency="USD")
XAUUSD_CASH = Contract(symbol="XAUUSD", secType="CASH", exchange="IDEALPRO", currency="USD")
XAUUSD_CFD = Contract(symbol="XAUUSD", secType="CFD", exchange="SMART", currency="USD")

# XAGUSD: Commodities, SMART, conId 77124483
XAGUSD_CMDTY = Contract(conId=77124483, symbol="XAGUSD", secType="CMDTY", exchange="SMART", currency="USD")
XAGUSD_CASH = Contract(symbol="XAGUSD", secType="CASH", exchange="IDEALPRO", currency="USD")
XAGUSD_CFD = Contract(symbol="XAGUSD", secType="CFD", exchange="SMART", currency="USD")

# Forex: IB expects symbol=base currency, currency=quote (e.g. EUR/USD). Use Forex() for correct format.
AUDUSD_CASH = Forex("AUDUSD")
EURUSD_CASH = Forex("EURUSD")

# Crypto: BTC/USD on PAXOS
BTCUSD_CRYPTO = Crypto("BTC", "PAXOS", "USD")

# Index: VIX (CBOE)
VIX_INDEX = Index("VIX", "CBOE", "USD")

# Brent crude oil: NYMEX BZ future — use config brent_con_id or brent_contract_month (roll when month expires).

# (display name, short name for message, decimal places, list of (contract, whatToShow) to try in order)
ASSETS_BASE = [
    ("XAUUSD", "XAU", 2, [(XAUUSD_CMDTY, "MIDPOINT"), (XAUUSD_CMDTY, "BID"), (XAUUSD_CMDTY, "TRADES")]),
    ("XAGUSD", "XAG", 3, [(XAGUSD_CMDTY, "MIDPOINT"), (XAGUSD_CMDTY, "BID"), (XAGUSD_CMDTY, "TRADES")]),
    ("AUD.USD", "AUD", 5, [(AUDUSD_CASH, "MIDPOINT"), (AUDUSD_CASH, "BID"), (AUDUSD_CASH, "TRADES"), (AUDUSD_CASH, "ASK")]),
    ("EUR.USD", "EUR", 5, [(EURUSD_CASH, "MIDPOINT"), (EURUSD_CASH, "BID"), (EURUSD_CASH, "TRADES"), (EURUSD_CASH, "ASK")]),
    ("BTC.USD", "BTC", 2, [(BTCUSD_CRYPTO, "MIDPOINT"), (BTCUSD_CRYPTO, "BID"), (BTCUSD_CRYPTO, "TRADES")]),
    ("BBG VIX", "BBG VIX", 2, [(VIX_INDEX, "MIDPOINT"), (VIX_INDEX, "BID"), (VIX_INDEX, "TRADES")]),
]


EXCLUDED_SHORTS = ("BTC", "BBG VIX")

def _brent_contract(config):
    """NYMEX BZ future. Prefer brent_con_id from TWS; else brent_contract_month YYYYMM (e.g. 202606)."""
    cid = config.get("brent_con_id")
    if cid is not None and str(cid).strip() != "":
        try:
            return Contract(conId=int(cid), symbol="BZ", secType="FUT", exchange="NYMEX", currency="USD")
        except (TypeError, ValueError):
            pass
    ym = config.get("brent_contract_month") or "202606"
    ym = str(ym).strip().replace("-", "")[:6]
    return Contract(symbol="BZ", secType="FUT", exchange="NYMEX", currency="USD", lastTradeDateOrContractMonth=ym)


def _asset_brent(config):
    c = _brent_contract(config)
    return ("Brent", "Brent", 2, [(c, "MIDPOINT"), (c, "BID"), (c, "TRADES")])


def _get_assets(config):
    """Return asset list; use CFD for XAU/XAG when use_xau_xag_cfd is true in config. Add Brent when use_brent is true."""
    use_cfd = bool(config.get("use_xau_xag_cfd"))
    if not use_cfd:
        out = [a for a in ASSETS_BASE if a[1] not in EXCLUDED_SHORTS]
    else:
        out = []
        for name, short, decimals, contract_what_list in ASSETS_BASE:
            if short in EXCLUDED_SHORTS:
                continue
            if name == "XAUUSD":
                contract_what_list = [(XAUUSD_CFD, w) for _, w in contract_what_list]
            elif name == "XAGUSD":
                contract_what_list = [(XAGUSD_CFD, w) for _, w in contract_what_list]
            out.append((name, short, decimals, contract_what_list))
    if config.get("use_brent"):
        out.append(_asset_brent(config))
    return out

def _load_config():
    """Load config.yaml. Returns full config dict or {}."""
    if not CONFIG_PATH.exists():
        return {}
    try:
        import yaml
        with open(CONFIG_PATH, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _parse_day(s):
    """Monday/Mon -> 0, Tuesday/Tue -> 1, ... Sunday/Sun -> 6."""
    days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    s = (s or "").strip().lower()[:3]
    for i, d in enumerate(days):
        if d.startswith(s) or d[:3] == s:
            return i
    return 0


def _parse_time(s):
    """'08:00' -> (8, 0)."""
    parts = (s or "00:00").strip().split(":")
    h = int(parts[0]) if parts else 0
    m = int(parts[1]) if len(parts) > 1 else 0
    return h, m


def _week_minutes(dt):
    """Minutes since Monday 00:00 in the same week (dt weekday 0=Mon). 0..10079."""
    return dt.weekday() * 24 * 60 + dt.hour * 60 + dt.minute


def _in_one_window(dt, window):
    """True if dt (HKT) falls inside this weekly window (start_time and end_time inclusive). Window can wrap (e.g. Mon 08:00 - Tue 03:00)."""
    start_day = _parse_day(window.get("start_day", "Monday"))
    end_day = _parse_day(window.get("end_day", "Tuesday"))
    sh, sm = _parse_time(window.get("start_time", "00:00"))
    eh, em = _parse_time(window.get("end_time", "00:00"))
    start_min = start_day * 24 * 60 + sh * 60 + sm
    end_min = end_day * 24 * 60 + eh * 60 + em
    m = _week_minutes(dt)
    if start_min <= end_min:
        return start_min <= m <= end_min
    return m >= start_min or m <= end_min


def _in_schedule(dt, schedule):
    """True if dt (HKT) falls inside any of the schedule windows. schedule is a list of window dicts or None."""
    if not schedule:
        return True
    return any(_in_one_window(dt, w) for w in schedule)


def _normalize_schedule(schedule):
    """Return schedule as list of windows, or None for 24/7."""
    if not schedule:
        return None
    return schedule if isinstance(schedule, list) else [schedule]


LOG_DIR = str(SCRIPT_DIR)
LOG_FMT = "%(asctime)s %(levelname)s %(message)s"
LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"


def _hkt_converter(t):
    return datetime.fromtimestamp(t, HKT).timetuple()


log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
log.handlers.clear()
console = logging.StreamHandler()
console_fmt = logging.Formatter(LOG_FMT, datefmt=LOG_DATEFMT)
console_fmt.converter = _hkt_converter
console.setFormatter(console_fmt)
log.addHandler(console)
log_filename = f"{LOG_DIR}/hourly_hl.log"
file_handler = logging.FileHandler(log_filename, encoding="utf-8")
file_fmt = logging.Formatter(LOG_FMT, datefmt=LOG_DATEFMT)
file_fmt.converter = _hkt_converter
file_handler.setFormatter(file_fmt)
log.addHandler(file_handler)


def fetch_last_completed_hour_hl(ib, contract_what_list):
    """Request the hourly bar that ends at the start of the current hour (HKT). Return (high, low, bar_start_dt in HKT) or (None, None, None). Bar start = start of that hour."""
    now_hkt = datetime.now(HKT)
    end_of_last_hour_hkt = now_hkt.replace(minute=0, second=0, microsecond=0)
    end_of_last_hour_utc = end_of_last_hour_hkt.astimezone(timezone.utc)
    req = dict(
        endDateTime=end_of_last_hour_utc,
        durationStr="3600 S",
        barSizeSetting="1 hour",
        useRTH=False,
        formatDate=1,
    )
    for contract, what in contract_what_list:
        try:
            bars = ib.reqHistoricalData(contract, whatToShow=what, **req)
            if bars:
                completed = bars[-1]
                bar_start = completed.date
                if hasattr(bar_start, "astimezone"):
                    if bar_start.tzinfo is None:
                        bar_start = bar_start.replace(tzinfo=timezone.utc).astimezone(HKT)
                    else:
                        bar_start = bar_start.astimezone(HKT)
                return completed.high, completed.low, bar_start
        except Exception:
            continue
    return None, None, None


def fetch_spot_price(ib, contract, decimals):
    """Fetch current spot-ish price using market data snapshot. Return rounded price or None."""
    try:
        # Use snapshot to avoid long-lived streaming subscriptions.
        ticker = ib.reqMktData(contract, "", True, False)
        # Give TWS a moment to populate bid/ask/last.
        ib.sleep(5)
        price = ticker.last
        if price is None or price == 0 or (isinstance(price, float) and math.isnan(price)):
            bid = ticker.bid
            ask = ticker.ask
            if bid is not None and ask is not None and not (math.isnan(bid) or math.isnan(ask)):
                price = (bid + ask) / 2
            elif bid is not None and not math.isnan(bid):
                price = bid
            elif ask is not None and not math.isnan(ask):
                price = ask
        try:
            ib.cancelMktData(contract)
        except Exception:
            pass
        if price is None or (isinstance(price, float) and math.isnan(price)):
            return None
        p = float(price)
        if p == -1.0:
            return None
        r = round(p, decimals)
        if r == -1.0:
            return None
        return r
    except Exception:
        return None


def run_once(whatsapp_number=None, send_whatsapp=True, assets=None, brent_multiplier=1.0):
    if assets is None:
        assets = ASSETS_BASE
    ib = IB()
    ib.RequestTimeout = 30
    lines = []
    now_hkt = datetime.now(HKT)
    expected_bar_start = (now_hkt - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    max_retries = 3
    retry_delay = 30  # seconds between connect retries
    for attempt in range(1, max_retries + 1):
        try:
            ib.connect(IB_HOST, IB_PORT, clientId=CLIENT_ID + attempt - 1)
            break
        except Exception as e:
            log.warning("IB connect attempt %d/%d failed: %s", attempt, max_retries, e)
            try:
                ib.disconnect()
            except Exception:
                pass
            if attempt < max_retries:
                log.info("Retrying in %d s ...", retry_delay)
                time.sleep(retry_delay)
            else:
                raise
    try:
        for name, short, decimals, contract_what_list in assets:
            high, low, bar_start = fetch_last_completed_hour_hl(ib, contract_what_list)
            if high is None or low is None:
                log.warning("%s past hour HL | no bar data", name)
                continue
            bar_start_hour = bar_start.replace(minute=0, second=0, microsecond=0) if hasattr(bar_start, "replace") else bar_start
            if bar_start_hour != expected_bar_start:
                log.warning("%s bar_start=%s not last hour (expected %s), skipping", name, bar_start, expected_bar_start)
                continue
            if short == "Brent":
                high, low = round(high * brent_multiplier, 2), round(low * brent_multiplier, 2)
            spot = None
            if short == "XAU":
                spot_contract = contract_what_list[0][0]
                spot = fetch_spot_price(ib, spot_contract, decimals)
                if spot is None:
                    log.warning("XAU spot unavailable; omitting spot in message")
            fmt = f"%.{decimals}f"
            if short == "XAU":
                if spot is None or (isinstance(spot, float) and math.isnan(spot)):
                    line = f"{short.lower()} {fmt % high} high {fmt % low} low"
                else:
                    line = f"{short.lower()} {fmt % spot} spot {fmt % high} high {fmt % low} low"
            else:
                line = f"{short.lower()} {fmt % high} high {fmt % low} low"
            log.info("%s past hour HL | bar_start=%s | high=%s | low=%s", name, bar_start, fmt % high, fmt % low)
            lines.append(line)
    finally:
        ib.disconnect()
    if lines and whatsapp_number and send_whatsapp:
        try:
            import pywhatkit as kit
            msg = "\n".join(lines)
            kit.sendwhatmsg_instantly(whatsapp_number, msg, wait_time=22, tab_close=True, close_time=15)
        except Exception as e:
            log.warning("WhatsApp send failed: %s", e)


def next_run_in_seconds(schedule):
    """Seconds until next run: 20 s past the hour (HKT), only when in schedule."""
    now = datetime.now(HKT)
    next_run = now.replace(minute=0, second=20, microsecond=0)
    next_run += timedelta(hours=1)
    # If we have a schedule, advance to next run time that falls inside the window
    if schedule:
        for _ in range(7 * 24 + 1):
            if _in_schedule(next_run, schedule):
                break
            next_run += timedelta(hours=1)
    return max(60, (next_run - now).total_seconds())


def main():
    config = _load_config()
    schedule = _normalize_schedule(config.get("schedule"))
    whatsapp_number = (config.get("whatsapp_number")).strip()
    assets = _get_assets(config)
    if config.get("use_xau_xag_cfd"):
        log.info("Using CFD source for XAUUSD and XAGUSD")
    if config.get("use_brent"):
        bc = _brent_contract(config)
        log.info("Brent (BZ) included | contract=%s", bc)
    if schedule:
        for i, w in enumerate(schedule):
            log.info("Schedule window %s: %s %s - %s %s (HKT)", i + 1, w.get("start_day"), w.get("start_time"), w.get("end_day"), w.get("end_time"))
    else:
        log.info("No config.yaml schedule; running 24/7")
    log.info("WhatsApp: %s", whatsapp_number)
    dont_send_now = bool(config.get("dont_send_now", True))
    brent_multiplier = float(config.get("brent_multiplier", 1.0))
    while True:
        now = datetime.now(HKT)
        if _in_schedule(now, schedule):
            try:
                run_once(whatsapp_number, send_whatsapp=not dont_send_now, assets=assets, brent_multiplier=brent_multiplier)
            except (TimeoutError, ConnectionError, OSError) as e:
                log.warning("IB connect/run failed, will retry next run: %s", e)
            dont_send_now = False
        else:
            log.info("Outside schedule window; skipping run")
            dont_send_now = False  # reset so the first run back in schedule sends WhatsApp
        wait = next_run_in_seconds(schedule)
        log.info("Next run in %.2f hrs (at 20 s past the hour HKT)", wait / 3600)
        time.sleep(wait)


if __name__ == "__main__":
    main()
