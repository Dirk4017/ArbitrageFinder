import logging
import sys
import os
import time
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

from selenium_stealth import stealth
from .oddsportal_scraper import OddsportalScraper
import undetected_chromedriver as uc
from selenium.webdriver.chrome.options import Options

from selenium.webdriver.common.by import By
options = Options()
options.add_argument("--disable-blink-features=AutomationControlled")
# Add more robust stealth
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
# Add a realistic screen resolution
options.add_argument("--window-size=1920,1080")

# Proxy setup - provide a working proxy URL here if available
PROXY_URL = None
if PROXY_URL:
    options.add_argument(f"--proxy-server={PROXY_URL}")

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

# Test fetching and scraping real URLs
sports_to_test = ["soccer", "tennis"]

try:
    for sport in sports_to_test:
        print(f"\n--- Testing {sport} ---")
        league_url = scraper.sports_config[sport]['leagues'][list(scraper.sports_config[sport]['leagues'].keys())[0]]
        print(f"Fetching URLs from {league_url}")

        match_urls = scraper._get_match_urls(league_url)
        print(f"Found {len(match_urls)} matches")

        # DEBUG: If no matches, print a sample of ALL links to identify the match pattern
        if len(match_urls) == 0 and sport == 'tennis':
            from bs4 import BeautifulSoup
            with open("league_page_debug.html", "r", encoding="utf-8") as f:
                soup = BeautifulSoup(f.read(), 'html.parser')
                links = soup.find_all('a', href=True)
                print(f"Found {len(links)} total links. Sample:")
                # Print 50 links to get a good idea of structure
                for link in links[:50]:
                    print(link.get('href'))

        for match_url in match_urls[:1]:
            print(f"URL to be scraped: {match_url}")
            # Try to navigate via session to check if the issue is with Selenium
            # Or perhaps the issue is the URL itself?
            # Let's try navigating to the match page directly.

            # Use driver to navigate.
            driver.get(match_url)
            time.sleep(10)

            # Print the URL again to see if it changed
            print(f"Current URL after driver.get: {driver.current_url}")

            # If it still redirects to homepage, the site might be blocking the driver
            # based on user agent, headers, or some other fingerprinting.
            print(f"Page Title: {driver.title}")
            # Dump page source to file to debug
            with open("test_debug_page_source.html", "w", encoding="utf-8") as f:
                f.write(driver.page_source)

            opportunities = scraper.scrape_match_odds(match_url, sport)
            print(f"Found {len(opportunities)} opportunities")
            for opp in opportunities[:3]:
                print(opp)
finally:
    driver.quit()
