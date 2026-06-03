"""
Ultra Stable Scanner - Enhanced version of your web scraper
"""
import re
import time
import random
import logging
import os
import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
# import chromedriver_autoinstaller_fix
# chromedriver_autoinstaller_fix.install()

from datetime import datetime, timedelta
from typing import Dict, List, Optional
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from .oddsportal_scraper import OddsportalScraper
from .odds_harvester_adapter import OddsHarvesterAdapter
from .odds_api_scraper import OddsAPIScraper
from core.config_manager import ConfigManager

logger = logging.getLogger(__name__)


class UltraStableScanner:
    def __init__(self, config: Optional[ConfigManager] = None):
        self.config = config or ConfigManager()
        self.driver = None
        self.use_demo_mode = False

        # Team name mappings for parsing
        self.team_variations = self._setup_team_mappings()

        # Initialize Oddsportal Scraper with config
        oddsportal_config = self.config.oddsportal.__dict__ if hasattr(self.config, 'oddsportal') else {}
        logger.info(f"DEBUG: Oddsportal config: {oddsportal_config}")
        logger.info(f"DEBUG: Value bets enabled: {self.config.oddsportal.value_bets_enabled if hasattr(self.config, 'oddsportal') else 'N/A'}")
        self.oddsportal_scraper = OddsportalScraper(config=oddsportal_config)

        # Initialize The Odds API Scraper
        api_key = self.config.api.odds_api_key if hasattr(self.config, 'api') else None
        sports_config = self.config.sports_config if hasattr(self.config, 'sports_config') else None
        self.odds_api_scraper = OddsAPIScraper(api_key=api_key, sports_config=sports_config)

    def reinitialize_webdriver(self, proxy: Optional[str] = None):
        """Callback to re-initialize the WebDriver upon session loss."""
        logger.warning(f"Re-initializing WebDriver with proxy: {proxy}...")
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None
        return self.initialize_webdriver(proxy=proxy)

    def _setup_team_mappings(self):
        """Setup comprehensive team name mappings"""
        return {
            'lakers': ['los angeles lakers', 'la lakers'],
            'warriors': ['golden state warriors', 'gsw'],
            'celtics': ['boston celtics'],
            'chiefs': ['kansas city chiefs'],
            'dolphins': ['miami dolphins'],
            'jets': ['new york jets'],
            # ... add more teams as needed
        }

    def initialize_webdriver(self, proxy: Optional[str] = None):
        """Ultra-stable WebDriver initialization with stealth enhancements."""
        try:
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
                self.driver = None

            chrome_options = uc.ChromeOptions()
            # Randomize window size
            width = random.randint(1280, 1920)
            height = random.randint(720, 1080)
            chrome_options.add_argument(f"--window-size={width},{height}")

            # Persistent User Data Directory
            user_data_dir = os.path.join(os.getcwd(), "chrome_profile")
            chrome_options.add_argument(f"--user-data-dir={user_data_dir}")

            # Essential stealth arguments
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-gpu")

            # Proxy support
            if proxy:
                chrome_options.add_argument(f"--proxy-server={proxy}")
                logger.info(f"Using proxy: {proxy}")

            # Initialize undetected_chromedriver
            # FIX: Allow auto-detection of chromedriver version to match Chrome browser
            driver = uc.Chrome(options=chrome_options, use_subprocess=True)

            # Stealth: Advanced fingerprinting masking
            stealth_script = """
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });

                // Mask plugins
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });

                // Mask languages
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en']
                });

                // Mask WebGL renderer
                const getParameter = WebGLRenderingContext.prototype.getParameter;
                WebGLRenderingContext.prototype.getParameter = function(parameter) {
                  if (parameter === 37445) return 'Intel Iris OpenGL Engine';
                  if (parameter === 37446) return 'Intel Inc.';
                  return getParameter.apply(this, arguments);
                };

                // Additional stealth properties
                Object.defineProperty(navigator, 'vendor', { get: () => 'Google Inc.' });
                Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
                window.chrome = { runtime: {} };
            """
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": stealth_script
            })

            driver.set_page_load_timeout(45)
            driver.implicitly_wait(15)

            logger.info("WebDriver initialized successfully with stealth enhancements")
            return driver
        except Exception as e:
            logger.error(f"WebDriver initialization failed: {e}")
            return None

    def scrape_crazyninja_safe(self) -> List[Dict]:
        """Safe scraping"""
        self.driver = self.initialize_webdriver()

        if not self.driver:
            logger.info("WebDriver unavailable")
            return []

        try:
            logger.info("Opening: https://crazyninjaodds.com/site/tools/positive-ev.aspx")
            self.driver.get("https://crazyninjaodds.com/site/tools/positive-ev.aspx")

            # Wait for page load with generous timeout
            WebDriverWait(self.driver, 30).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )

            logger.info("Betting content loaded successfully")
            time.sleep(3)  # Allow JavaScript to load

            # Try to parse opportunities
            opportunities = self.parse_opportunities_safe()

            # Always close driver after use
            self.driver.quit()
            self.driver = None

            return opportunities

        except Exception as e:
            logger.error(f"Scraping failed: {e}")
            # Close driver on failure
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
                self.driver = None
            return []

    def parse_opportunities_safe(self) -> List[Dict]:
        """Safe parsing with player name extraction"""
        try:
            if not self.driver:
                return []

            tables = self.driver.find_elements(By.TAG_NAME, "table")
            logger.info(f"Found {len(tables)} tables")

            if not tables or len(tables) == 0:
                return []

            opportunities = []
            # Try to parse the main table (usually first one)
            main_table = tables[0]
            rows = main_table.find_elements(By.TAG_NAME, "tr")

            logger.info(f"Found {len(rows)} rows in main table")

            # Process a limited number of rows
            max_rows_to_process = min(15, len(rows) - 1)  # Skip header

            for i in range(1, max_rows_to_process + 1):
                try:
                    row = rows[i]
                    cells = row.find_elements(By.TAG_NAME, "td")

                    if len(cells) >= 11:  # Need at least 11 columns
                        # Extract data using correct column indices
                        ev_text = cells[0].text.strip('%')  # LW-WC EV%
                        date_text = cells[3].text if len(cells) > 3 else ''
                        sport = cells[4].text if len(cells) > 4 else 'Unknown'
                        event = cells[6].text if len(cells) > 6 else 'Unknown'
                        market = cells[7].text
                        player_name = cells[8].text
                        odds = cells[9].text if len(cells) > 9 else 'Unknown'
                        sportsbook = cells[10].text if len(cells) > 10 else 'Unknown'

                        # Extract date
                        game_date = self._parse_website_date(date_text, sport)

                        # Append date to event for R script
                        event_with_date = f"{event} {game_date}"

                        try:
                            ev_value = float(ev_text) / 100
                        except:
                            ev_value = 0.0

                        # Only include valid opportunities
                        if ev_value >= 0.02 and odds and odds != 'Unknown':
                            opportunity = {
                                'ev': ev_value,
                                'sport': sport,
                                'event': event_with_date,
                                'market': market,
                                'player': player_name,
                                'odds': odds,
                                'sportsbook': sportsbook,
                                'game_date': game_date,
                                'raw_date': date_text
                            }
                            opportunities.append(opportunity)
                            logger.info(f"Found: {sport} | {player_name} - {market} @ {odds} | EV: +{ev_value:.1%}")

                except Exception as e:
                    logger.warning(f"Error parsing row {i}: {e}")
                    continue

            if opportunities:
                logger.info(f"Successfully parsed {len(opportunities)} opportunities from website")
                return opportunities
            else:
                logger.info("No valid opportunities found")
                return []

        except Exception as e:
            logger.error(f"Parsing error: {e}")
            return self.generate_realistic_opportunities()

    def _parse_website_date(self, date_text: str, sport: str) -> str:
        """Parse German date formats correctly"""
        try:
            date_text = date_text.strip()

            if not date_text:
                return datetime.now().strftime('%Y-%m-%d')

            current_year = datetime.now().year
            current_month = datetime.now().month

            # Check for explicit year
            year_match = re.search(r'(\d{4})', date_text)
            if year_match:
                current_year = int(year_match.group(1))

            # Try different date formats
            date_formats = [
                # German formats
                r'(\d{1,2})\.\s+(\w+)\.',  # "14. Dez."
                r'\w+\.\s+(\d{1,2})\.\s+(\w+)\.',  # "So., 14. Dez."
                # English formats
                r'(\w+)\s+(\d{1,2})',  # "Dec 14"
                # Numeric formats
                r'(\d{1,2})/(\d{1,2})/(\d{4})',  # "12/14/2025"
                r'(\d{1,2})/(\d{1,2})',  # "12/14"
            ]

            for pattern in date_formats:
                match = re.search(pattern, date_text, re.IGNORECASE)
                if match:
                    # Try to parse based on pattern
                    try:
                        if '/' in pattern:
                            # Handle MM/DD/YYYY or MM/DD
                            parts = [p for p in match.groups() if p]
                            if len(parts) == 2:
                                month, day = int(parts[0]), int(parts[1])
                                year = self._infer_correct_year(month, current_year, current_month)
                                return f"{year}-{month:02d}-{day:02d}"
                            elif len(parts) == 3:
                                month, day, year = int(parts[0]), int(parts[1]), int(parts[2])
                                return f"{year}-{month:02d}-{day:02d}"
                        else:
                            # Handle text month formats
                            month_map = {
                                'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
                                'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
                            }

                            parts = [p for p in match.groups() if p]
                            if len(parts) >= 2:
                                # Find month and day
                                for part in parts:
                                    if part.lower()[:3] in month_map:
                                        month = month_map[part.lower()[:3]]
                                        # The other part should be day
                                        for other in parts:
                                            if other != part and other.isdigit():
                                                day = int(other)
                                                year = self._infer_correct_year(month, current_year, current_month)
                                                return f"{year}-{month:02d}-{day:02d}"
                    except:
                        continue

            # If all parsing fails, use tomorrow
            tomorrow = datetime.now() + timedelta(days=1)
            return tomorrow.strftime('%Y-%m-%d')

        except Exception as e:
            logger.error(f"Date parsing error: {e}")
            # Default to tomorrow for future games
            tomorrow = datetime.now() + timedelta(days=1)
            return tomorrow.strftime('%Y-%m-%d')

    def _infer_correct_year(self, month_num: int, current_year: int, current_month: int) -> int:
        """Infer the correct year for a date based on month"""
        # If month is December (12) and current month is Jan-Jun, it's previous year
        if month_num == 12 and current_month <= 6:
            return current_year - 1

        # If month is January (1) and current month is Dec, it's next year
        if month_num == 1 and current_month == 12:
            return current_year + 1

        # If month is earlier in year than current month (and not crossing year boundary)
        if month_num < current_month:
            # Check if we might be looking at next year's schedule
            if current_month >= 10 and month_num <= 3:
                return current_year + 1
            else:
                return current_year

        return current_year

    def generate_realistic_opportunities(self) -> List[Dict]:
        """Generate realistic +EV opportunities for testing"""
        logger.info("GENERATING REALISTIC +EV OPPORTUNITIES")

        events = [
            {"sport": "NFL", "event": "Bengals @ Ravens", "market": "Ja'Marr Chase Receiving Yards",
             "player": "Ja'Marr Chase", "odds": "+185", "ev": 0.085, "sportsbook": "DraftKings"},
            {"sport": "NFL", "event": "Packers @ Lions", "market": "Jared Goff Passing Yards",
             "player": "Jared Goff", "odds": "-115", "ev": 0.042, "sportsbook": "FanDuel"},
            {"sport": "NBA", "event": "Lakers @ Warriors", "market": "LeBron James Points",
             "player": "LeBron James", "odds": "-110", "ev": 0.045, "sportsbook": "BetMGM"},
        ]

        for event in events:
            logger.info(f"Realistic: {event['event']} | {event['player']} - {event['market']} @ {event['odds']}")

        return events

    def scrape_crazyninja_odds(self) -> List[Dict]:
        """Main scraping method - always returns opportunities"""
        return self.scrape_crazyninja_safe()

    def scrape_all_sports(self) -> List[Dict]:
        """
        Scrapes opportunities for all configured sports using The Odds API (primary)
        and Oddsportal (secondary/backup).
        """
        logger.info("Starting real-time multi-sport scan...")
        all_opportunities = []

        # 1. Try Oddsportal (Web scraping)
        try:
            logger.info("Falling back to Oddsportal scraping...")

            # Ensure driver is initialized for Oddsportal
            if not self.driver:
                self.driver = self.initialize_webdriver()

            if self.driver:
                # Inject driver into the scraper
                self.oddsportal_scraper.driver = self.driver
                opportunities = self.oddsportal_scraper.scrape_all_sports()
                if opportunities:
                    all_opportunities.extend(opportunities)
            else:
                logger.warning("Could not initialize WebDriver for Oddsportal")
        except Exception as e:
            logger.error(f"Oddsportal scraping failed: {e}")

        # 2. Fallback to simulated data only if everything else fails
        if not all_opportunities:
            logger.info("No real data found")

        return all_opportunities

    def scrape_oddsportal_opportunities(self) -> List[Dict]:
        """Wrapper to call scrape_all_sports for compatibility."""
        return self.scrape_all_sports()

    def scrape_oddsportal_value_bets(self) -> List[Dict]:
        """Scrape Oddsportal Value Bets page for +EV opportunities"""
        if not self.oddsportal_scraper:
            logger.warning("Oddsportal scraper not initialized")
            return []

        try:
            opportunities = self.oddsportal_scraper.scrape_value_bets()
            logger.info(f"Found {len(opportunities)} opportunities from Oddsportal Value Bets")
            return opportunities
        except Exception as e:
            logger.error(f"Oddsportal Value Bets scraping failed: {e}")
            return []

    def close(self):
        """Close the webdriver"""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None