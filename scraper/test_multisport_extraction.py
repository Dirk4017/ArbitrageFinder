import logging
import sys
import os
import time
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from selenium_stealth import stealth
from .oddsportal_scraper import OddsportalScraper
import undetected_chromedriver as uc
from selenium.webdriver.chrome.options import Options

options = Options()
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--window-size=1920,1080")

driver = uc.Chrome(options=options)

stealth(driver,
        languages=["en-US", "en"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
        )

scraper = OddsportalScraper(driver=driver)

# Test fetching and scraping Cricket, Rugby, and MMA
sports_to_test = ["cricket", "rugby", "mma"]

try:
    for sport in sports_to_test:
        print(f"\n--- Testing {sport} ---")
        if sport not in scraper.sports_config:
            print(f"Sport {sport} not in configuration.")
            continue

        league_url = scraper.sports_config[sport]['leagues'][list(scraper.sports_config[sport]['leagues'].keys())[0]]
        print(f"Fetching URLs from {league_url}")

        match_urls = scraper._get_match_urls(league_url)
        print(f"Found {len(match_urls)} matches")

        for match_url in match_urls[:1]:
            print(f"URL to be scraped: {match_url}")
            opportunities = scraper.scrape_match_odds(match_url, sport)
            print(f"Found {len(opportunities)} opportunities")
            for opp in opportunities[:3]:
                print(opp)
finally:
    driver.quit()
