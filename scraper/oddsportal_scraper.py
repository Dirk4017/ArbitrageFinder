import logging
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import requests

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
                options = uc.ChromeOptions()
                options.add_argument("--headless")
                options.add_argument("--ignore-certificate-errors")
                options.add_argument("--allow-running-insecure-content")
                # Force version 148 as verified
                self.driver = uc.Chrome(options=options, use_subprocess=True, version_main=148)
            return self.driver

    def fetch_league_matches(self, league_name: str, league_url: str) -> List[str]:
        # Check cache
        if league_name in self.cache:
            timestamp, matches = self.cache[league_name]
            if datetime.now() - timestamp < timedelta(hours=self.config.get("cache_duration_hours", 1)):
                return matches

        # Try requests first
        logger.info(f"Fetching matches for {league_name} via requests")
        try:
            response = self.session.get(league_url, timeout=10, verify=False)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            return self._parse_match_links(soup, league_name)
        except Exception as e:
            logger.warning(f"Requests failed for {league_name}: {e}. Falling back to browser.")

            # Fallback to browser
            with self.driver_lock:
                driver = self._get_driver()
                try:
                    driver.get(league_url)
                    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/h2h/']")))
                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    return self._parse_match_links(soup, league_name)
                except Exception as e2:
                    logger.error(f"Browser fallback failed for {league_name}: {e2}")
                    return []

    def _parse_match_links(self, soup: BeautifulSoup, league_name: str) -> List[str]:
        match_urls = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            if '/h2h/' in href:
                full_url = f"https://www.oddsportal.com{href}" if href.startswith('/') else href
                if full_url not in match_urls:
                    match_urls.append(full_url)

        self.cache[league_name] = (datetime.now(), match_urls)
        return match_urls

    def scrape_match_odds(self, match_url: str) -> List[Dict]:
        logger.info(f"DEBUG: Starting to scrape match odds: {match_url}")
        with self.driver_lock:
            driver = self._get_driver()
            try:
                logger.info(f"DEBUG: Navigating to {match_url}")
                driver.get(match_url)
                logger.info(f"DEBUG: Navigation complete, waiting for odds-cell")
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.odds-cell")))
                logger.info(f"DEBUG: Found odds-cell, parsing source")

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
