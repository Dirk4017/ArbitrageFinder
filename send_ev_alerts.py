import asyncio
import logging
import os
import requests
from scraper.odds_harvester_adapter import OddsHarvesterAdapter
from scraper.ultra_stable_scanner import UltraStableScanner

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Config from environment variables (or hardcoded for testing/dev)
BOT_TOKEN = "8629707064:AAHlIeU8TEe--jrJ4Isnc6sccoad-1xqFt0"
FREE_CHANNEL = "-1003938799273"
PREMIUM_CHANNEL = "-1003950531572"

def send_message(chat_id, text):
    if not BOT_TOKEN or not chat_id:
        logger.error("Missing bot token or chat ID")
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown"
    }
    try:
        response = requests.post(url, json=payload)
        if not response.ok:
            logger.error(f"Failed to send to Telegram: {response.text}")
    except Exception as e:
        logger.error(f"Failed to send to Telegram: {e}")

def get_opportunities():
    opportunities = []

    # 1. Get from CrazyNinja (Scanner)
    try:
        scanner = UltraStableScanner()
        opportunities.extend(scanner.scrape_crazyninja_odds())
    except Exception as e:
        logger.error(f"Error scraping CrazyNinja: {e}")

    # 2. Get from Oddsportal (Harvester)
    try:
        harvester = OddsHarvesterAdapter()
        opportunities.extend(harvester.scrape_all())
    except Exception as e:
        logger.error(f"Error scraping Oddsportal: {e}")

    return opportunities

def main():
    logger.info("Running alert scan...")
    opportunities = get_opportunities()
    logger.info(f"Found {len(opportunities)} opportunities total.")

    for opp in opportunities:
        # Normalize EV data
        try:
            ev = float(opp.get('ev', 0))
        except (ValueError, TypeError):
            continue

        if ev <= 0:
            continue

def calculate_kelly_stake(ev, decimal_odds):
    """
    Calculate fractional Kelly stake.
    Formula: ( (odds - 1) * prob - (1 - prob) ) / (odds - 1)
    Here, 'prob' is (implied_prob + ev_edge).
    Simplified for +EV: (EV / (odds - 1)) * fraction
    Using 0.25 (quarter) Kelly for safety.
    """
    try:
        # Convert American odds to decimal if necessary
        # Assuming odds is a string like '+170' or '-115' or a float/int string
        # For this simple bot, we'll assume decimal odds are passed or simple conversion

        # Simple American to Decimal conversion for display/calc
        odds_val = float(odds) if isinstance(odds, (int, float)) else 0.0
        # If it's a string '+170', parse it
        if isinstance(odds, str):
            if odds.startswith('+'):
                odds_val = (int(odds[1:]) / 100) + 1
            elif odds.startswith('-'):
                odds_val = (100 / int(odds[1:])) + 1
            else:
                odds_val = float(odds)

        if odds_val <= 1:
            return 0.0

        # Kelly Stake = (Edge / Odds-1) * Fraction
        kelly = (ev / (odds_val - 1)) * 0.25
        return min(kelly, 0.05) # Cap at 5% of bankroll
    except:
        return 0.0

def main():
    logger.info("Running alert scan...")
    opportunities = get_opportunities()
    logger.info(f"Found {len(opportunities)} opportunities total.")

    for opp in opportunities:
        # Normalize EV data
        try:
            ev = float(opp.get('ev', 0))
        except (ValueError, TypeError):
            continue

        if ev <= 0:
            continue

        sport = opp.get('sport', 'Unknown')
        event = opp.get('event', 'Unknown')
        selection = opp.get('player', opp.get('team', 'Unknown'))
        odds = opp.get('odds', 'N/A')
        bookmaker = opp.get('sportsbook', 'Unknown')

        stake = calculate_kelly_stake(ev, odds)

        message = (f"Sport: {sport}\n"
                   f"Event: {event}\n"
                   f"Selection: {selection}\n"
                   f"EV: +{ev*100:.1f}%\n"
                   f"Odds: {odds}\n"
                   f"Bookmaker: {bookmaker}\n"
                   f"Rec. Stake: {stake*100:.1f}% of Bankroll")

        # Send alerts
        if ev >= 0.08: # 8% EV threshold for premium
            send_message(PREMIUM_CHANNEL, f"PREMIUM ALERT\n{message}")
        else:
            send_message(FREE_CHANNEL, message)

        # Add a small delay to avoid hitting Telegram API limits
        import time
        time.sleep(1)

if __name__ == "__main__":
    main()
