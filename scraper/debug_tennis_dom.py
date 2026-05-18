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
from selenium.webdriver.common.by import By

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

# Inspect a known tennis match page to see DOM structure
tennis_match_url = "https://www.oddsportal.com/tennis/france/atp-french-open/djokovic-novak-nadal-rafael-CjD0s9qj/"

try:
    print(f"\n--- Inspecting {tennis_match_url} ---")
    driver.get(tennis_match_url)
    time.sleep(15)

    # Dump page source to check what we got
    with open("tennis_match_debug.html", "w", encoding="utf-8") as f:
        f.write(driver.page_source)
    print("Page source dumped to tennis_match_debug.html")

    # Try finding elements manually
    rows = driver.find_elements(By.CSS_SELECTOR, "div.flex.h-9.border-b.border-l.border-r.border-black-borders.text-xs")
    print(f"Found {len(rows)} CSS rows")

    rows_xpath = driver.find_elements(By.XPATH, "//div[.//div[contains(@class, 'odds-cell')]]")
    print(f"Found {len(rows_xpath)} XPath rows")

    # Print a sample of the page source to check structure
    print(f"Page title: {driver.title}")

finally:
    driver.quit()
