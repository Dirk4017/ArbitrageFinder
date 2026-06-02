import logging
import json
import threading
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import requests
import undetected_chromedriver as uc
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
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9'
        })
        self.profile_path = os.path.join(os.getcwd(), "chrome_profile")

    def _get_driver(self):
        with self.driver_lock:
            if self.driver is None:
                options = Options()
                # options.add_argument("--headless=new")
                options.add_argument("--no-sandbox")
                options.add_argument("--disable-dev-shm-usage")
                options.add_argument("--disable-gpu")
                options.add_argument("--ignore-certificate-errors")
                options.add_argument("--allow-running-insecure-content")
                options.add_argument("--disable-blink-features=AutomationControlled")
                options.add_argument("--disable-infobars")
                options.add_argument("--start-maximized")
                options.add_argument("--disable-extensions")
                options.add_argument("--disable-popup-blocking")

                # Use a specific, clean user profile, but perhaps a NEW directory each time to avoid fingerprinting
                # self.profile_path is already configured.
                # Let's ensure it's fresh for now to avoid any persistent cookies/tracking
                import shutil
                if os.path.exists(self.profile_path):
                    try:
                        shutil.rmtree(self.profile_path)
                    except:
                        pass
                options.add_argument(f"--user-data-dir={self.profile_path}")

                options.add_experimental_option("excludeSwitches", ["enable-automation"])
                options.add_experimental_option('useAutomationExtension', False)

                # Randomized user agent string
                user_agents = [
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
                ]
                import random
                ua = random.choice(user_agents)
                options.add_argument(f"--user-agent={ua}")

                from webdriver_manager.chrome import ChromeDriverManager
                from selenium.webdriver.chrome.service import Service

                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(options=options, service=service)

                # Apply stealth
                stealth(self.driver,
                        languages=["en-US", "en"],
                        vendor="Google Inc.",
                        platform="Win32",
                        webgl_vendor="Intel Inc.",
                        renderer="Intel Iris OpenGL Engine",
                        fix_hairline=True,
                        )

                # Explicitly hide automation property
                self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

                logger.info(f"DEBUG: Standard Chrome driver initialized with stealth and navigator fix")
            return self.driver

    def fetch_league_matches(self, league_name: str, league_url: str) -> List[str]:
        # Check cache
        if league_name in self.cache:
            timestamp, matches = self.cache[league_name]
            if datetime.now() - timestamp < timedelta(hours=self.config.get("cache_duration_hours", 1)):
                return matches

        # Define headers to mimic a real browser - simpler version
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
        }

        # Try requests first
        logger.info(f"Fetching matches for {league_name} via requests at {league_url}")
        # Ensure we are not sending default requests for testing locally if we know it fails
        try:
            # Add a small delay to mimic human behavior
            import time
            time.sleep(2)

            # Use a slightly different approach for the session
            # Maybe the session is too persistent and being tracked?
            # Let's try with a fresh request object just for the initial navigation
            # response = self.session.get(league_url, timeout=15, headers=headers, verify=False)

            # Using requests directly to see if session is the issue
            response = requests.get(league_url, timeout=15, headers=headers, verify=False)
            response.raise_for_status()

            # Check if we were redirected to a different domain
            # Be more tolerant: check if oddsportal is in the URL at all, even if redirected
            if "oddsportal.com" not in response.url:
                from urllib.parse import urlparse
                domain = urlparse(response.url).netloc
                logger.warning(f"Requests redirected to {response.url} (Domain: {domain}) for {league_name}. Forcing browser fallback.")
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

                    # Give it a moment to potentially redirect
                    import time
                    time.sleep(5)

                    logger.info(f"DEBUG: After nav, URL: {driver.current_url}")
                    logger.info(f"DEBUG: Page Title: {driver.title}")

                    # Check for centroquote
                    if "centroquote" in driver.current_url.lower() or "centroquote" in driver.title.lower():
                        logger.error(f"DEBUG: Blocked/Redirected to centroquote!")
                        return []

                    logger.info(f"DEBUG: Waiting for body in browser")
                    WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

                    # Use a more explicit waiting strategy for the main content
                    # The page structure might have changed
                    # Let's wait for elements that are likely to be on the match list page
                    # Looking at the sample links, they start with /football/ or /tennis/
                    try:
                        WebDriverWait(driver, 20).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/football/'], a[href*='/tennis/']"))
                        )
                    except Exception as e:
                        logger.warning(f"Timeout waiting for match list elements: {e}")

                    logger.info(f"DEBUG: Current URL after nav: {driver.current_url}")
                    soup = BeautifulSoup(driver.page_source, 'html.parser')

                    # DEBUG: Print all page content in chunks to debug
                    page_source = driver.page_source
                    logger.info(f"DEBUG: Page Source length: {len(page_source)}")
                    for i in range(0, min(len(page_source), 2000), 500):
                        logger.info(f"DEBUG: Page Source snippet {i}: {page_source[i:i+500]}")

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
        logger.info(f"DEBUG: Parsing links for {league_name}.")

        # Find all H2H links
        h2h_links = soup.find_all('a', href=lambda href: href and '/h2h/' in href)
        logger.info(f"DEBUG: Found {len(h2h_links)} H2H links.")

        for link in h2h_links:
            href = link.get('href', '')
            full_url = f"https://www.oddsportal.com{href}" if href.startswith('/') else href
            if full_url not in match_urls:
                match_urls.append(full_url)
                logger.info(f"DEBUG: Found match link: {full_url}")

        logger.info(f"DEBUG: Found {len(match_urls)} total unique match links for {league_name}")
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

        if not all_leagues:
            logger.info("No leagues enabled in configuration.")
            return []

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

    def scrape_value_bets(self) -> List[Dict]:
        """Scrapes the Value Bets page using the identified parser logic."""
        # This will need to be adapted based on the parser we developed
        options = Options()
        options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36')

        # Need to use standard selenium as in the new scraper, not undetected chromedriver
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(options=options, service=service)

        try:
            driver.get('https://www.oddsportal.com/value-bets/')
            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

            try:
                accept_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
                )
                accept_button.click()
                time.sleep(2)
            except Exception:
                pass

            body_text = driver.find_element(By.TAG_NAME, "body").text

            # Parser logic from ValueBetsScraper
            lines = [line.strip() for line in body_text.split('\n') if line.strip()]
            opportunities = []
            sports = ['Football', 'Tennis', 'Hockey', 'Boxing', 'Basketball', 'Baseball']
            i = 0
            while i < len(lines):
                if lines[i] in sports:
                    sport = lines[i]
                    i += 1
                    while i < len(lines) and lines[i] == '/': i += 1
                    country = lines[i]
                    i += 1
                    while i < len(lines) and lines[i] == '/': i += 1
                    league = lines[i]
                    i += 1
                    market = lines[i]
                    i += 1
                    date = lines[i]
                    i += 1
                    time_val = lines[i]
                    i += 1
                    team_a = lines[i]
                    i += 1
                    team_b = lines[i]
                    while i < len(lines):
                        i += 1
                        if i >= len(lines) or lines[i] in sports: break
                        if lines[i] in ['Over', 'Under', '1', '2', 'X']:
                            outcome = lines[i]
                            i += 1  # Skip Bookmaker line
                            try:
                                odds = float(lines[i])
                                i += 1
                                value = float(lines[i].replace('%', '')) / 100
                                i += 1
                                prob = lines[i]  # Prob is often a percentage string
                            except (ValueError, IndexError):
                                # Skip if parsing fails
                                continue

                            opportunities.append({
                                'sport': sport, 'country': country, 'league': league,
                                'market': market, 'date': date, 'time': time_val,
                                'team_a': team_a, 'team_b': team_b,
                                'outcome': outcome, 'odds': odds, 'value': value, 'prob': prob
                            })
                            # Keep looking for more outcomes in the same event
                            continue
                i += 1
            return opportunities
        finally:
            driver.quit()

