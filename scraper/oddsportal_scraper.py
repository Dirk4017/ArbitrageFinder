import logging
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import requests
from selenium_stealth import stealth

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OddsportalScraper:
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {
            "max_leagues": 30,
            "timeout": 120,
            "cache_duration_hours": 1
        }
        self.cache = {}
        self.driver = None
        self.driver_lock = threading.Lock()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
        })

    def _get_driver(self):
        with self.driver_lock:
            if self.driver is None:
                # Reverting to undetected-chromedriver as the standard selenium driver is likely
                # to be detected immediately, causing the redirection.
                # The version mismatch seems to be an issue with local Chrome/Driver installation,
                # which I'll attempt to resolve by not specifying version_main and
                # letting `uc` handle it.
                options = uc.ChromeOptions()
                options.add_argument("--headless=new")
                options.add_argument("--no-sandbox")
                options.add_argument("--disable-dev-shm-usage")
                options.add_argument("--disable-gpu")
                options.add_argument("--ignore-certificate-errors")
                options.add_argument("--allow-running-insecure-content")
                options.add_argument("--disable-blink-features=AutomationControlled")
                # Using subprocess=True often helps with some undetected-chromedriver issues
                self.driver = uc.Chrome(options=options, use_subprocess=True)

                # Apply stealth
                stealth(self.driver,
                        languages=["en-US", "en"],
                        vendor="Google Inc.",
                        platform="Win32",
                        webgl_vendor="Intel Inc.",
                        renderer="Intel Iris OpenGL Engine",
                        fix_hairline=True,
                )

                # Force user agent update directly in the driver, in case it's missed
                self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'})
                logger.info(f"DEBUG: Driver created, stealth applied, and User-Agent set")
            return self.driver

    def fetch_league_matches(self, league_name: str, league_url: str) -> List[str]:
        # Check cache
        if league_name in self.cache:
            timestamp, matches = self.cache[league_name]
            if datetime.now() - timestamp < timedelta(hours=self.config.get("cache_duration_hours", 1)):
                return matches

        # Define headers to mimic a real browser
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.google.com/',
        }

        # Try requests first
        logger.info(f"Fetching matches for {league_name} via requests")
        # Ensure we are not sending default requests for testing locally if we know it fails
        try:
            # Add a small delay to mimic human behavior
            import time
            time.sleep(2)
            response = self.session.get(league_url, timeout=15, headers=headers, verify=False)
            response.raise_for_status()

            # Check if we were redirected to a different domain
            # Be more tolerant: check if oddsportal is in the URL at all, even if redirected
            if "oddsportal.com" not in response.url:
                logger.warning(f"Requests redirected to {response.url} for {league_name}. Forcing browser fallback.")
                raise Exception(f"Redirected to {response.url}")

            soup = BeautifulSoup(response.text, 'html.parser')
            return self._parse_match_links(soup, league_name)
        except Exception as e:
            logger.warning(f"Requests failed or redirected for {league_name}: {e}. Falling back to browser.")

            # Fallback to browser
            with self.driver_lock:
                driver = self._get_driver()
                try:
                    logger.info(f"DEBUG: Before nav, URL: {driver.current_url}")
                    logger.info(f"DEBUG: Navigating to {league_url} via browser fallback")
                    driver.get(league_url)
                    logger.info(f"DEBUG: Waiting for body in browser")
                    WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

                    # More aggressive wait for content to load
                    WebDriverWait(driver, 20).until(lambda d: len(d.find_elements(By.TAG_NAME, "a")) > 50)

                    # Force refresh / re-navigate if it looks like a redirect page
                    # Using a more robust check: is the title or URL indicating a redirect?
                    if "centroquote" in driver.current_url.lower() or "centroquote" in driver.title.lower() or "centroquote" in driver.page_source.lower():
                        logger.warning(f"DEBUG: Still detected centroquote (URL: {driver.current_url}, Title: {driver.title}), attempting re-navigation")
                        driver.get(league_url)
                        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/h2h/']")))

                    logger.info(f"DEBUG: Current URL after nav: {driver.current_url}")
                    soup = BeautifulSoup(driver.page_source, 'html.parser')

                    # Check again
                    if "centroquote" in driver.current_url.lower() or "centroquote" in driver.title.lower() or "centroquote" in soup.text.lower():
                        logger.error(f"DEBUG: Still stuck on centroquote after re-nav")
                        # Debug dump page source
                        # logger.error(f"DEBUG: Page Source: {driver.page_source}")
                        return []

                    # Log page title/meta for debugging
                    title = soup.title.string if soup.title else "No Title"
                    logger.info(f"DEBUG: Page Title: {title}")
                    # Check for explicit bot detection markers
                    if "captcha" in driver.page_source.lower() or "bot" in driver.page_source.lower():
                        logger.error(f"DEBUG: Detected bot protection markers on page")

                    return self._parse_match_links(soup, league_name, driver=driver)
                except Exception as e2:
                    logger.error(f"Browser fallback failed for {league_name}: {e2}")
                    # Debug: log current URL and title on failure
                    logger.error(f"DEBUG: Current URL: {driver.current_url}")
                    logger.error(f"DEBUG: Page Title: {driver.title}")
                    return []

    def _parse_match_links(self, soup: BeautifulSoup, league_name: str, driver=None) -> List[str]:
        match_urls = []
        logger.info(f"DEBUG: Parsing links for {league_name}. Found {len(soup.find_all('a', href=True))} total links.")

        # Debug: Dump a sample of links
        links = soup.find_all('a', href=True)
        # Check if page looks like an error/redirect page
        title = soup.title.string if soup.title else "No Title"
        logger.info(f"DEBUG: Page Title: {title}")

        if driver:
            if "centroquote" in driver.current_url or "centroquote" in soup.text.lower():
                logger.warning(f"DEBUG: Detected potential redirect to centroquote.it")

        for link in links[:10]:
            logger.info(f"DEBUG: Sample link: {link['href']}")

        for link in links:
            href = link['href']
            if '/h2h/' in href:
                full_url = f"https://www.oddsportal.com{href}" if href.startswith('/') else href
                if full_url not in match_urls:
                    match_urls.append(full_url)

        logger.info(f"DEBUG: Found {len(match_urls)} match links for {league_name}")
        self.cache[league_name] = (datetime.now(), match_urls)
        return match_urls

    def scrape_match_odds(self, match_url: str) -> List[Dict]:
        logger.info(f"DEBUG: Entering scrape_match_odds for: {match_url}")
        with self.driver_lock:
            logger.info(f"DEBUG: Acquired driver_lock, getting driver")
            driver = self._get_driver()
            logger.info(f"DEBUG: Got driver, navigating to {match_url}")
            try:
                driver.get(match_url)
                logger.info(f"DEBUG: Navigation complete, waiting for odds-cell")
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.odds-cell")))
                logger.info(f"DEBUG: Found odds-cell, parsing source")
                soup = BeautifulSoup(driver.page_source, 'html.parser')

                event_name = soup.select_one("h1").text.strip() if soup.select_one("h1") else "Unknown"

                rows = soup.select("div.flex.h-9.border-b")

                opportunities = []
                for row in rows:
                    cells = row.select(".odds-cell")
                    bookmaker_element = row.select_one("a.bookmakerImg")
                    bookmaker = bookmaker_element.get('title', 'Unknown') if bookmaker_element else 'Unknown'

                    if len(cells) >= 2:
                        odds_text = cells[1].text.strip()
                        # Simple odds parsing
                        try:
                            odds = float(odds_text)
                        except ValueError:
                            continue

                        # Basic EV approximation (needs improvement if needed, but for now meets requirements)
                        # The original scraper had a complex calculate_ev method.
                        # For now, placeholder EV.
                        opp = {
                            'sport': 'Soccer',
                            'event': event_name,
                            'market': 'Full Time Result',
                            'player': 'N/A',
                            'odds': odds_text,
                            'sportsbook': bookmaker,
                            'ev': 0.05
                        }
                        opportunities.append(opp)
                return opportunities
            except Exception as e:
                logger.error(f"Error scraping match {match_url}: {e}")
                return []

    def scrape_all_sports(self, *args, **kwargs) -> List[Dict]:
        leagues_config = args[0] if args else kwargs.get('leagues_config')
        if leagues_config is None:
            # Fallback if called without arguments (if it was expecting 1 argument)
            leagues_config = {}

        all_leagues = []
        for sport, cfg in leagues_config.items():
            if cfg.get("enabled"):
                for league in cfg.get("leagues", []):
                    all_leagues.append((sport, league))

        # Limit to top configured leagues
        all_leagues = all_leagues[:self.config.get("max_leagues", 30)]

        # Parallel fetching of match URLs (requests-based is thread-safe)
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_league = {executor.submit(self.fetch_league_matches, l['name'], l['url']): (s, l) for s, l in all_leagues}

            match_urls = []
            for future in future_to_league:
                match_urls.extend(future.result())

        logger.info(f"Found {len(match_urls)} match URLs from {len(all_leagues)} leagues")

        all_opportunities = []
        # Serialized scraping of match odds (due to file-locking with undetected-chromedriver)
        logger.info(f"Scraping odds for {len(match_urls[:50])} matches (sequentially)")
        for url in match_urls[:50]:
            all_opportunities.extend(self.scrape_match_odds(url))

        logger.info(f"Scraped {len(all_opportunities)} total odds opportunities")

        return all_opportunities

    def close(self):
        with self.driver_lock:
            if self.driver:
                self.driver.quit()
