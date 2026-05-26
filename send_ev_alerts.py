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

        sport = opp.get('sport', 'Unknown')
        event = opp.get('event', 'Unknown')
        selection = opp.get('player', opp.get('team', 'Unknown'))
        odds = opp.get('odds', 'N/A')
        bookmaker = opp.get('sportsbook', 'Unknown')

        message = f"Sport: {sport}\nEvent: {event}\nSelection: {selection}\nEV: +{ev*100:.1f}%\nOdds: {odds}\nBookmaker: {bookmaker}"

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
