import warnings
warnings.warn("OddsportalScraper is deprecated. Use OddsHarvesterAdapter instead.", DeprecationWarning, stacklevel=2)

import requests
from bs4 import BeautifulSoup
import time
import re
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional
import random
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import os

logger = logging.getLogger(__name__)

class OddsportalScraper:
    """
    Scraper for oddsportal.com focusing on +EV opportunities for Soccer and other sports.
    Compares Soft Bookmakers against Market Average (Media) when Pinnacle is unavailable.
    """

    def __init__(self, config: Optional[Dict] = None, driver=None, reinit_callback=None, proxy_list: Optional[List[str]] = None):
        self.config = config or {}
        self.driver = driver
        self.reinit_callback = reinit_callback
        self.proxy_list = proxy_list or []
        self.current_proxy = None
        self.min_ev = self.config.get('min_ev', 0.05)  # Default 5% EV
        self.delay_range = (5, 10)

        # ... rest of init
        self._rotate_proxy()

    def _rotate_proxy(self):
        if self.proxy_list:
            self.current_proxy = random.choice(self.proxy_list)
            logger.info(f"Rotated to proxy: {self.current_proxy}")
            # Note: Rotating proxy in Selenium after initialization is difficult
            # and often requires restarting the driver, which is handled by reinit_callback.

        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36'
        ]

        self.session = requests.Session()
        self._rotate_user_agent()

        self.sports_config = {
            'soccer': {
                'leagues': {
                    'argentina-copa-de-la-liga-profesional': 'https://www.oddsportal.com/football/argentina/copa-de-la-liga-profesional/',
                    'argentina-primera-nacional': 'https://www.oddsportal.com/football/argentina/primera-nacional/',
                    'armenia-premier-league': 'https://www.oddsportal.com/football/armenia/premier-league/',
                    'australia-a-league': 'https://www.oddsportal.com/football/australia/a-league/',
                    'austria-bundesliga': 'https://www.oddsportal.com/football/austria/bundesliga/',
                    'austria-2-liga': 'https://www.oddsportal.com/football/austria/2-liga/',
                    'azerbaijan-premier-league': 'https://www.oddsportal.com/football/azerbaijan/premier-league/',
                    'bahrain-premier-league': 'https://www.oddsportal.com/football/bahrain/premier-league/',
                    'belarus-vysshaya-liga': 'https://www.oddsportal.com/football/belarus/vysshaya-liga/',
                    'belgium-challenger-pro-league': 'https://www.oddsportal.com/football/belgium/challenger-pro-league/',
                    'belgium-jupiler-pro-league': 'https://www.oddsportal.com/football/belgium/jupiler-pro-league/',
                    'bolivia-division-profesional': 'https://www.oddsportal.com/football/bolivia/division-profesional/',
                    'bosnia-and-herzegovina-premijer-liga-bih': 'https://www.oddsportal.com/football/bosnia-and-herzegovina/premijer-liga-bih/',
                    'brazil-serie-a': 'https://www.oddsportal.com/football/brazil/serie-a/',
                    'brazil-serie-b': 'https://www.oddsportal.com/football/brazil/serie-b/',
                    'bulgaria-parva-liga': 'https://www.oddsportal.com/football/bulgaria/parva-liga/',
                    'chile-primera-division': 'https://www.oddsportal.com/football/chile/primera-division/',
                    'chile-primera-b': 'https://www.oddsportal.com/football/chile/primera-b/',
                    'china-super-league': 'https://www.oddsportal.com/football/china/super-league/',
                    'colombia-primera-b': 'https://www.oddsportal.com/football/colombia/primera-b/',
                    'colombia-primera-a': 'https://www.oddsportal.com/football/colombia/primera-a/',
                    'costa-rica-primera-division': 'https://www.oddsportal.com/football/costa-rica/primera-division/',
                    'croatia-prva-nl': 'https://www.oddsportal.com/football/croatia/prva-nl/',
                    'croatia-hnl': 'https://www.oddsportal.com/football/croatia/hnl/',
                    'cyprus-cyta-championship': 'https://www.oddsportal.com/football/cyprus/cyta-championship/',
                    'czech-republic-fnl': 'https://www.oddsportal.com/football/czech-republic/fnl/',
                    'czech-republic-fortuna-liga': 'https://www.oddsportal.com/football/czech-republic/fortuna-liga/',
                    'denmark-1st-division': 'https://www.oddsportal.com/football/denmark/1st-division/',
                    'denmark-superliga': 'https://www.oddsportal.com/football/denmark/superliga/',
                    'ecuador-liga-pro': 'https://www.oddsportal.com/football/ecuador/liga-pro/',
                    'egypt-premier-league': 'https://www.oddsportal.com/football/egypt/premier-league/',
                    'england-league-two': 'https://www.oddsportal.com/football/england/league-two/',
                    'england-league-one': 'https://www.oddsportal.com/football/england/league-one/',
                    'england-premier-league': 'https://www.oddsportal.com/football/england/premier-league/',
                    'england-championship': 'https://www.oddsportal.com/football/england/championship/',
                    'england-national-league': 'https://www.oddsportal.com/football/england/national-league/',
                    'estonia-meistriliiga': 'https://www.oddsportal.com/football/estonia/meistriliiga/',
                    'finland-ykkonen': 'https://www.oddsportal.com/football/finland/ykkonen/',
                    'finland-veikkausliiga': 'https://www.oddsportal.com/football/finland/veikkausliiga/',
                    'france-ligue-1': 'https://www.oddsportal.com/football/france/ligue-1/',
                    'france-national': 'https://www.oddsportal.com/football/france/national/',
                    'france-ligue-2': 'https://www.oddsportal.com/football/france/ligue-2/',
                    'germany-bundesliga': 'https://www.oddsportal.com/football/germany/bundesliga/',
                    'germany-2-bundesliga': 'https://www.oddsportal.com/football/germany/2-bundesliga/',
                    'germany-3-liga': 'https://www.oddsportal.com/football/germany/3-liga/',
                    'greece-super-league': 'https://www.oddsportal.com/football/greece/super-league/',
                    'honduras-liga-nacional': 'https://www.oddsportal.com/football/honduras/liga-nacional/',
                    'hungary-otp-bank-liga': 'https://www.oddsportal.com/football/hungary/otp-bank-liga/',
                    'iceland-besta-deild-karla': 'https://www.oddsportal.com/football/iceland/besta-deild-karla/',
                    'india-isl': 'https://www.oddsportal.com/football/india/isl/',
                    'ireland-division-1': 'https://www.oddsportal.com/football/ireland/division-1/',
                    'ireland-premier-division': 'https://www.oddsportal.com/football/ireland/premier-division/',
                    'israel-leumit-league': 'https://www.oddsportal.com/football/israel/leumit-league/',
                    'israel-ligat-ha-al': 'https://www.oddsportal.com/football/israel/ligat-ha-al/',
                    'italy-serie-b': 'https://www.oddsportal.com/football/italy/serie-b/',
                    'italy-serie-a': 'https://www.oddsportal.com/football/italy/serie-a/',
                    'japan-j1-league': 'https://www.oddsportal.com/football/japan/j1-league/',
                    'japan-j2-league': 'https://www.oddsportal.com/football/japan/j2-league/',
                    'jordan-premier-league': 'https://www.oddsportal.com/football/jordan/premier-league/',
                    'kazakhstan-premier-league': 'https://www.oddsportal.com/football/kazakhstan/premier-league/',
                    'kuwait-premier-league': 'https://www.oddsportal.com/football/kuwait/premier-league/',
                    'lithuania-a-lyga': 'https://www.oddsportal.com/football/lithuania/a-lyga/',
                    'malaysia-super-league': 'https://www.oddsportal.com/football/malaysia/super-league/',
                    'malta-premier-league': 'https://www.oddsportal.com/football/malta/premier-league/',
                    'mexico-liga-mx': 'https://www.oddsportal.com/football/mexico/liga-mx/',
                    'montenegro-prva-crnogorska-liga': 'https://www.oddsportal.com/football/montenegro/prva-crnogorska-liga/',
                    'morocco-botola-pro': 'https://www.oddsportal.com/football/morocco/botola-pro/',
                    'netherlands-eredivisie': 'https://www.oddsportal.com/football/netherlands/eredivisie/',
                    'netherlands-eerste-divisie': 'https://www.oddsportal.com/football/netherlands/eerste-divisie/',
                    'northern-ireland-nifl-premiership': 'https://www.oddsportal.com/football/northern-ireland/nifl-premiership/',
                    'norway-obos-ligaen': 'https://www.oddsportal.com/football/norway/obos-ligaen/',
                    'norway-eliteserien': 'https://www.oddsportal.com/football/norway/eliteserien/',
                    'paraguay-primera-division': 'https://www.oddsportal.com/football/paraguay/primera-division/',
                    'peru-liga-1': 'https://www.oddsportal.com/football/peru/liga-1/',
                    'poland-division-1': 'https://www.oddsportal.com/football/poland/division-1/',
                    'poland-ekstraklasa': 'https://www.oddsportal.com/football/poland/ekstraklasa/',
                    'portugal-liga-portugal': 'https://www.oddsportal.com/football/portugal/liga-portugal/',
                    'portugal-liga-portugal-2': 'https://www.oddsportal.com/football/portugal/liga-portugal-2/',
                    'qatar-qsl': 'https://www.oddsportal.com/football/qatar/qsl/',
                    'romania-liga-2': 'https://www.oddsportal.com/football/romania/liga-2/',
                    'romania-liga-1': 'https://www.oddsportal.com/football/romania/liga-1/',
                    'russia-fnl': 'https://www.oddsportal.com/football/russia/fnl/',
                    'russia-premier-league': 'https://www.oddsportal.com/football/russia/premier-league/',
                    'saudi-arabia-saudi-professional-league': 'https://www.oddsportal.com/football/saudi-arabia/saudi-professional-league/',
                    'scotland-league-two': 'https://www.oddsportal.com/football/scotland/league-two/',
                    'scotland-league-one': 'https://www.oddsportal.com/football/scotland/league-one/',
                    'scotland-championship': 'https://www.oddsportal.com/football/scotland/championship/',
                    'scotland-premiership': 'https://www.oddsportal.com/football/scotland/premiership/',
                    'serbia-super-liga': 'https://www.oddsportal.com/football/serbia/super-liga/',
                    'slovakia-nike-liga': 'https://www.oddsportal.com/football/slovakia/nike-liga/',
                    'slovenia-prva-liga': 'https://www.oddsportal.com/football/slovenia/prva-liga/',
                    'south-africa-premier-league': 'https://www.oddsportal.com/football/south-africa/premier-league/',
                    'south-korea-k-league-2': 'https://www.oddsportal.com/football/south-korea/k-league-2/',
                    'south-korea-k-league-1': 'https://www.oddsportal.com/football/south-korea/k-league-1/',
                    'spain-laliga': 'https://www.oddsportal.com/football/spain/laliga/',
                    'spain-laliga2': 'https://www.oddsportal.com/football/spain/laliga2/',
                    'sweden-superettan': 'https://www.oddsportal.com/football/sweden/superettan/',
                    'sweden-allsvenskan': 'https://www.oddsportal.com/football/sweden/allsvenskan/',
                    'switzerland-challenge-league': 'https://www.oddsportal.com/football/switzerland/challenge-league/',
                    'switzerland-super-league': 'https://www.oddsportal.com/football/switzerland/super-league/',
                    'thailand-thai-league-1': 'https://www.oddsportal.com/football/thailand/thai-league-1/',
                    'turkey-1-lig': 'https://www.oddsportal.com/football/turkey/1-lig/',
                    'turkey-super-lig': 'https://www.oddsportal.com/football/turkey/super-lig/',
                    'ukraine-premier-league': 'https://www.oddsportal.com/football/ukraine/premier-league/',
                    'united-arab-emirates-uae-league': 'https://www.oddsportal.com/football/united-arab-emirates/uae-league/',
                    'uruguay-primera-division': 'https://www.oddsportal.com/football/uruguay/primera-division/',
                    'usa-usl-championship': 'https://www.oddsportal.com/football/usa/usl-championship/',
                    'usa-mls': 'https://www.oddsportal.com/football/usa/mls/',
                    'venezuela-liga-futve': 'https://www.oddsportal.com/football/venezuela/liga-futve/',
                    'vietnam-v-league-1': 'https://www.oddsportal.com/football/vietnam/v-league-1/',
                    'wales-cymru-premier': 'https://www.oddsportal.com/football/wales/cymru-premier/'
                },
                'market_map': {
                    '1x2': 'Full Time Result',
                    'home_away': 'Moneyline',
                    'over_under': 'Total Goals',
                    'handicap': 'Asian Handicap',
                    'esito finale': 'Full Time Result',
                    'più/meno': 'Total Goals',
                    'media': 'Average'
                }
            },
            'tennis': {
                'leagues': {
                    'atp': 'https://www.oddsportal.com/tennis/atp/',
                    'wta': 'https://www.oddsportal.com/tennis/wta/',
                },
                'market_map': {
                    'h2h': 'Match Result',
                    'media': 'Average'
                }
            },
            'volleyball': {
                'leagues': {
                    'champions-league': 'https://www.oddsportal.com/volleyball/europe/champions-league/'
                },
                'market_map': {
                    'h2h': 'Match Result',
                    'media': 'Average'
                }
            },
            'cricket': {
                'leagues': {
                    'ipl': 'https://www.oddsportal.com/cricket/india/ipl/'
                },
                'market_map': {
                    'h2h': 'Match Result',
                    'media': 'Average'
                }
            },
            'mma': {
                'leagues': {
                    'ufc': 'https://www.oddsportal.com/mma/usa/ufc/'
                },
                'market_map': {
                    'h2h': 'Match Result',
                    'media': 'Average'
                }
            },
            'rugby': {
                'leagues': {
                    'premiership': 'https://www.oddsportal.com/rugby-union/england/premiership/'
                },
                'market_map': {
                    'h2h': 'Match Result',
                    'media': 'Average'
                }
            },
            'horse-racing': {
                'leagues': {
                    'uk-ireland': 'https://www.oddsportal.com/horse-racing/uk-ireland/',
                    'usa': 'https://www.oddsportal.com/horse-racing/usa/',
                    'australia': 'https://www.oddsportal.com/horse-racing/australia/'
                },
                'market_map': {'h2h': 'Winner', 'media': 'Average'}
            },
            'golf': {
                'leagues': {
                    'pga-tour': 'https://www.oddsportal.com/golf/usa/pga-tour/',
                    'dp-world-tour': 'https://www.oddsportal.com/golf/europe/dp-world-tour/',
                    'majors': 'https://www.oddsportal.com/golf/majors/'
                },
                'market_map': {'h2h': 'Winner', 'media': 'Average'}
            },
            'cycling': {
                'leagues': {
                    'tour-de-france': 'https://www.oddsportal.com/cycling/france/tour-de-france/',
                    'giro-d-italia': 'https://www.oddsportal.com/cycling/italy/giro-d-italia/',
                    'vuelta-a-espana': 'https://www.oddsportal.com/cycling/spain/vuelta-a-espana/'
                },
                'market_map': {'h2h': 'Winner', 'media': 'Average'}
            },
            'esports': {
                'leagues': {
                    'league-of-legends': 'https://www.oddsportal.com/esports/league-of-legends/',
                    'cs-go': 'https://www.oddsportal.com/esports/cs-go/',
                    'dota-2': 'https://www.oddsportal.com/esports/dota-2/',
                    'valorant': 'https://www.oddsportal.com/esports/valorant/',
                    'call-of-duty': 'https://www.oddsportal.com/esports/call-of-duty/'
                },
                'market_map': {'h2h': 'Match Result', 'media': 'Average'}
            },
            'boxing': {
                'leagues': {
                    'heavyweight': 'https://www.oddsportal.com/boxing/heavyweight/',
                    'light-heavyweight': 'https://www.oddsportal.com/boxing/light-heavyweight/',
                    'middleweight': 'https://www.oddsportal.com/boxing/middleweight/',
                    'welterweight': 'https://www.oddsportal.com/boxing/welterweight/',
                    'lightweight': 'https://www.oddsportal.com/boxing/lightweight/',
                    'featherweight': 'https://www.oddsportal.com/boxing/featherweight/'
                },
                'market_map': {'h2h': 'Winner', 'media': 'Average'}
            },
            'motorsports': {
                'leagues': {
                    'formula-1': 'https://www.oddsportal.com/motorsports/formula-1/',
                    'formula-2': 'https://www.oddsportal.com/motorsports/formula-2/',
                    'formula-3': 'https://www.oddsportal.com/motorsports/formula-3/',
                    'motogp': 'https://www.oddsportal.com/motorsports/motogp/',
                    'nascar': 'https://www.oddsportal.com/motorsports/nascar/',
                    'indycar': 'https://www.oddsportal.com/motorsports/indycar/',
                    'world-rally': 'https://www.oddsportal.com/motorsports/world-rally/'
                },
                'market_map': {'h2h': 'Winner', 'media': 'Average'}
            },
            'snooker': {
                'leagues': {
                    'world-championship': 'https://www.oddsportal.com/snooker/world-championship/',
                    'uk-championship': 'https://www.oddsportal.com/snooker/uk-championship/',
                    'masters': 'https://www.oddsportal.com/snooker/masters/',
                    'ranking-events': 'https://www.oddsportal.com/snooker/'
                },
                'market_map': {'h2h': 'Match Result', 'media': 'Average'}
            },
            'darts': {
                'leagues': {
                    'pdc-world-championship': 'https://www.oddsportal.com/darts/pdc-world-championship/',
                    'premier-league': 'https://www.oddsportal.com/darts/premier-league/',
                    'world-matchplay': 'https://www.oddsportal.com/darts/world-matchplay/',
                    'grand-slam': 'https://www.oddsportal.com/darts/grand-slam/'
                },
                'market_map': {'h2h': 'Match Result', 'media': 'Average'}
            },
            'lacrosse': {
                'leagues': {
                    'pll': 'https://www.oddsportal.com/lacrosse/usa/pll/',
                    'nll': 'https://www.oddsportal.com/lacrosse/usa/nll/',
                    'world-championship': 'https://www.oddsportal.com/lacrosse/world-championship/'
                },
                'market_map': {'h2h': 'Match Result', 'media': 'Average'}
            },
            'australian-rules': {
                'leagues': {
                    'afl': 'https://www.oddsportal.com/australian-rules/afl/'
                },
                'market_map': {'h2h': 'Match Result', 'media': 'Average'}
            },
            'handball': {
                'leagues': {
                    'world-championship': 'https://www.oddsportal.com/handball/world-championship/',
                    'european-championship': 'https://www.oddsportal.com/handball/european-championship/',
                    'champions-league': 'https://www.oddsportal.com/handball/champions-league/'
                },
                'market_map': {'h2h': 'Match Result', 'media': 'Average'}
            },
            'water-polo': {
                'leagues': {
                    'world-championship': 'https://www.oddsportal.com/water-polo/world-championship/',
                    'olympic-games': 'https://www.oddsportal.com/water-polo/olympic-games/',
                    'euro-league': 'https://www.oddsportal.com/water-polo/euro-league/'
                },
                'market_map': {'h2h': 'Match Result', 'media': 'Average'}
            },
            'table-tennis': {
                'leagues': {
                    'ittf-world-tour': 'https://www.oddsportal.com/table-tennis/ittf-world-tour/',
                    'world-championship': 'https://www.oddsportal.com/table-tennis/world-championship/'
                },
                'market_map': {'h2h': 'Match Result', 'media': 'Average'}
            },
            'badminton': {
                'leagues': {
                    'bwf-world-tour': 'https://www.oddsportal.com/badminton/bwf-world-tour/',
                    'world-championship': 'https://www.oddsportal.com/badminton/world-championship/',
                    'thomas-uber-cup': 'https://www.oddsportal.com/badminton/thomas-uber-cup/',
                    'sudirman-cup': 'https://www.oddsportal.com/badminton/sudirman-cup/',
                    'olympic-games': 'https://www.oddsportal.com/badminton/olympic-games/'
                },
                'market_map': {'h2h': 'Match Result', 'media': 'Average'}
            },
            'floorball': {
                'leagues': {
                    'world-championship': 'https://www.oddsportal.com/floorball/world-championship/',
                    'champions-cup': 'https://www.oddsportal.com/floorball/champions-cup/'
                },
                'market_map': {'h2h': 'Match Result', 'media': 'Average'}
            },
            'futsal': {
                'leagues': {
                    'world-cup': 'https://www.oddsportal.com/futsal/world-cup/',
                    'champions-league': 'https://www.oddsportal.com/futsal/champions-league/'
                },
                'market_map': {'h2h': 'Match Result', 'media': 'Average'}
            },
            'beach-volleyball': {
                'leagues': {
                    'world-tour': 'https://www.oddsportal.com/beach-volleyball/world-tour/',
                    'world-championship': 'https://www.oddsportal.com/beach-volleyball/world-championship/'
                },
                'market_map': {'h2h': 'Match Result', 'media': 'Average'}
            },
            'winter-sports': {
                'leagues': {
                    'alpine-skiing': 'https://www.oddsportal.com/winter-sports/alpine-skiing/',
                    'biathlon': 'https://www.oddsportal.com/winter-sports/biathlon/',
                    'speed-skating': 'https://www.oddsportal.com/winter-sports/speed-skating/',
                    'figure-skating': 'https://www.oddsportal.com/winter-sports/figure-skating/'
                },
                'market_map': {'h2h': 'Winner', 'media': 'Average'}
            },
            'bandy': {
                'leagues': {
                    'world-championship': 'https://www.oddsportal.com/bandy/world-championship/'
                },
                'market_map': {'h2h': 'Match Result', 'media': 'Average'}
            },
            'surfing': {
                'leagues': {
                    'wsl-championship-tour': 'https://www.oddsportal.com/surfing/wsl-championship-tour/'
                },
                'market_map': {'h2h': 'Winner', 'media': 'Average'}
            },

        }

        self.soft_bookmakers = [
            'Bet365', 'Betway', 'William Hill', 'Bwin', 'Unibet', '888Sport', 'Interwetten', 'Betfair',
            'Sisal', 'Snai', 'Goldbet', 'Eurobet', 'Marathonbet', 'Pinnacle', 'ComeOn', 'LeoVegas',
            'Betclic', 'NetBet', 'Mr Green'
        ]

    def scrape_all_sports(self) -> List[Dict]:
        all_opportunities = []

        for sport, config in self.sports_config.items():
            leagues = config['leagues']
            for league_name, url in leagues.items():
                self._rotate_user_agent()
                logger.info(f"Scraping {sport} league: {league_name}")
                try:
                    match_urls = self._get_match_urls(url)
                    logger.info(f"Found {len(match_urls)} matches in {league_name}")

                    for match_url in match_urls[:10]:
                        self._apply_delay()
                        match_opps = self.scrape_match_odds(match_url, sport)
                        all_opportunities.extend(match_opps)

                except Exception as e:
                    logger.error(f"Error scraping {sport} league {league_name}: {e}")

        return all_opportunities

    def _get_match_urls(self, league_url: str) -> List[str]:
        try:
            if self.driver:
                self.driver.get(league_url)
                logger.info(f"Final URL after loading: {self.driver.current_url}")
                # Wait for match links to load
                try:
                    WebDriverWait(self.driver, 20).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/h2h/']"))
                    )
                except TimeoutException:
                    logger.warning(f"Timeout waiting for match links on {league_url}")
                    # DEBUG: Print all links if timeout occurs
                    all_links = self.driver.find_elements(By.TAG_NAME, "a")
                    logger.debug(f"Found {len(all_links)} links on the page:")
                    for link in all_links[:20]:
                        logger.debug(f"Link: {link.get_attribute('href')}")
                html = self.driver.page_source
            else:
                response = self.session.get(league_url)
                if response.status_code != 200:
                    logger.error(f"Request failed with status {response.status_code} for {league_url}")
                    return []
                html = response.text

            # Dump league page source for debugging
            with open("league_page_debug.html", "w", encoding="utf-8") as f:
                f.write(html)
            logger.info("League page source dumped to league_page_debug.html")

            soup = BeautifulSoup(html, 'html.parser')
            # Check for error page
            if "www.oddsportal.com" == soup.title.string or "Access Denied" in html:
                logger.warning(f"Likely blocked or redirected on {league_url}")
                return []

            # Log a small snippet to see what's in the soup
            logger.info(f"Page title: {soup.title.string if soup.title else 'No Title'}")
            match_urls = []

            # Check if there are any match links at all to determine if it's empty
            all_links = soup.find_all('a', href=True)
            if not any('/h2h/' in link.get('href') for link in all_links):
                logger.info(f"No upcoming matches found for {league_url} - skipping")
                return []

            # NEW: Extract from JavaScript script tags first
            scripts = soup.find_all('script')
            logger.info(f"Found {len(scripts)} script tags")
            for script in scripts:
                if not script.string: continue
                # Log snippet of first 500 chars to debug
                logger.debug(f"Script content snippet: {script.string[:500]}")

                # NEW: Try JSON-LD first for robustness
                if 'application/ld+json' in script.get('type', ''):
                    try:
                        data = json.loads(script.string)
                        if isinstance(data, dict): data = [data]
                        for entry in data:
                            if isinstance(entry, dict) and 'url' in entry and 'centroquote.it' in entry['url']:
                                logger.info(f"Adding URL from JSON-LD: {entry['url']}")
                                match_urls.append(entry['url'])
                    except json.JSONDecodeError:
                        logger.debug("Failed to parse JSON-LD script tag")

                if ('SportsEvent' in script.string or 'h2h' in script.string):
                    # Extract URLs using regex pattern for centroquote.it match pages
                    # Updated regex to be more flexible, capturing the URL and handling potential escaped slashes
                    pattern = r'"url":"(https:\\\/\\\/www\.centroquote\.it\\\/[a-zA-Z0-9_\-]+\\\/h2h\\\/[^"]+)"'
                    urls = re.findall(pattern, script.string)
                    # Fix the escaped slashes if they exist
                    fixed_urls = [u.replace('\\/', '/') for u in urls]

                    # Also check if it's already unescaped
                    # Some URLs might not include "www." in the script data
                    pattern2 = r'"url":"(https://(?:www\.)?centroquote\.it/[a-zA-Z0-9_\-]+/h2h/[^"]+)"'
                    urls2 = re.findall(pattern2, script.string)

                    all_urls = fixed_urls + urls2
                    if all_urls:
                        logger.info(f"Adding URLs from regex: {all_urls}")
                    match_urls.extend(all_urls)

            js_count = len(match_urls)
            logger.info(f"Found {js_count} match URLs via JS extraction")

            # Remove duplicates
            match_urls = list(set(match_urls))

            # Filter out the redirection homepage
            match_urls = [url for url in match_urls if "centroquote.it" not in url or url != "https://centroquote.it"]

            # Debug: what did we find
            logger.info(f"Match URLs found after filtering: {match_urls}")

            # Fallback to HTML link parsing
            if not match_urls:
                logger.info("No match URLs found via JS, falling back to HTML parsing")
                supported_sports = "|".join(self.sports_config.keys())
                # Refined pattern to catch tennis H2H links which may have extra segments
                # Matches URLs like: /tennis/h2h/player1/player2/inplay-odds/
                # OR tennis match links: /tennis/league/player1-vs-player2-id/
                pattern = rf'/(?:soccer|football|{supported_sports})/h2h/[a-zA-Z0-9_\-]+/[a-zA-Z0-9_\-]+(?:/[a-zA-Z0-9_\-]+)?/|/tennis/[a-zA-Z0-9_\-]+/[a-zA-Z0-9_\-]+/'
                links = soup.find_all('a', href=re.compile(pattern))
                for link in links:
                    href = link.get('href')
                    if href:
                        # Full URL
                        # Ensure we convert relative to absolute oddsportal URL
                        if href.startswith('/'):
                            full_url = f"https://www.oddsportal.com{href}"
                        else:
                            full_url = href

                        # Avoid CentroQuote redirection/obfuscation target
                        if "centroquote.it" in full_url:
                            continue

                        # The URL should be valid for scraping
                        match_urls.append(full_url)
                match_urls = list(set(match_urls))
                logger.info(f"Found {len(match_urls) - js_count} match URLs via HTML fallback")

            return match_urls

        except Exception as e:
            logger.error(f"Error getting match URLs: {e}")
            return []

    def _rotate_user_agent(self):
        ua = random.choice(self.user_agents)
        self.session.headers.update({'User-Agent': ua})

    def _transfer_cookies(self):
        """Transfers cookies from Selenium driver to requests session."""
        if not self.driver:
            return

        cookies = self.driver.get_cookies()
        for cookie in cookies:
            self.session.cookies.set(cookie['name'], cookie['value'], domain=cookie.get('domain'))
        logger.info(f"Transferred {len(cookies)} cookies from driver to session.")

    def scrape_match_odds(self, match_url: str, sport: str) -> List[Dict]:
        """Scrape odds for a specific match from H2H page and calculate EV."""
        if not self.driver:
            return []

        # Ensure correct H2H URL structure
        if "/h2h/" not in match_url:
            sport_path = 'football' if sport == 'soccer' else sport
            # Ensure we only replace the sport path once
            match_url = match_url.replace(f"/{sport_path}/", f"/{sport_path}/h2h/", 1)

        if "#" in match_url:
            match_url = match_url.split("#")[0]

        # Add CDP/stealth tweaks
        self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });

                // Block/fake common anti-bot libraries and properties
                window.navigator.chrome = { runtime: {} };
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });

                // Emulate canvas to avoid simple fingerprinting
                const getContext = HTMLCanvasElement.prototype.getContext;
                HTMLCanvasElement.prototype.getContext = function(type) {
                    if (type === 'webgl' || type === 'webgl2') {
                        const gl = getContext.apply(this, arguments);
                        const getParameter = gl.getParameter;
                        gl.getParameter = function(name) {
                            if (name === 37445) return 'Google Inc. (Intel)';
                            if (name === 37446) return 'ANGLE (Intel, Intel(R) Iris(R) Xe Graphics, OpenGL 4.5)';
                            return getParameter.apply(this, arguments);
                        };
                        return gl;
                    }
                    return getContext.apply(this, arguments);
                };

                // Modify navigator properties
                Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
                Object.defineProperty(navigator, 'vendor', { get: () => 'Google Inc.' });
                Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 8 });

                // Add fake screen properties
                Object.defineProperty(screen, 'width', { get: () => 1920 });
                Object.defineProperty(screen, 'height', { get: () => 1080 });
                Object.defineProperty(screen, 'availWidth', { get: () => 1920 });
                Object.defineProperty(screen, 'availHeight', { get: () => 1080 });
            """
        })

        # Add more random delays to simulate human behavior
        time.sleep(random.uniform(5, 10))

        try:
            logger.info(f"Scraping match odds from: {match_url}")

            # Stealth: Add custom headers via CDP before navigation
            # Removing Referer and Sec-Fetch-Site to look more like a fresh start
            self.driver.execute_cdp_cmd("Network.setExtraHTTPHeaders", {
                "headers": {
                    "Accept-Language": "en-US,en;q=0.9",
                    "User-Agent": random.choice(self.user_agents),
                    "Upgrade-Insecure-Requests": "1"
                }
            })

            # Navigate to homepage first to establish session
            logger.info("Navigating to homepage first...")
            self.driver.get("https://www.oddsportal.com/")
            time.sleep(random.uniform(15, 25)) # Increased delay further to be extra safe

            # Instead of immediate URL check, maybe wait for a specific element
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.header-logo"))
                )
            except TimeoutException:
                logger.warning("Homepage didn't load expected elements. Redirection likely.")

            # Check for immediate redirection upon homepage load
            if "centroquote.it" in self.driver.current_url:
                logger.warning("Homepage redirected! Trying to clear cookies and retry.")
                self.driver.delete_all_cookies()
                self.driver.refresh()
                time.sleep(10)
                if "centroquote.it" in self.driver.current_url:
                    logger.error("Still redirected after cookie clear. Blocked.")
                    return []

            # Log navigation steps
            logger.info(f"Navigating to {match_url}...")
            # Use direct navigation after browsing homepage first,
            # this is often less suspicious than script-based navigation.
            self.driver.get(match_url)
            time.sleep(random.uniform(7, 10))

            # Add a small delay to capture the URL immediately after navigation
            time.sleep(2)
            current_url_after_get = self.driver.current_url
            logger.info(f"Current URL after navigation: {current_url_after_get}")

            if "centroquote.it" in current_url_after_get:
                logger.error(f"FATAL: Redirected to CentroQuote! Likely blocked by anti-bot.")
                # If redirected, try using a different approach: click a link if possible
                # or just accept that we are blocked for this session.
                # Since we can't easily click a link here, we will try to refresh.
                self.driver.refresh()
                time.sleep(5)
                logger.info(f"After refresh: {self.driver.current_url}")

            # DEBUG: Log current URL and page title to check for redirects/blocks
            logger.info(f"Navigated to: {self.driver.current_url}")
            logger.info(f"Page title: {self.driver.title}")

            # Check for immediate blocking signatures in page source
            if "Access Denied" in self.driver.page_source or "challenges" in self.driver.current_url:
                logger.warning(f"Detected potential block on {match_url}. Current URL: {self.driver.current_url}")

            # Wait for content to load
            WebDriverWait(self.driver, 20).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            time.sleep(15)

            # Handle cookie/privacy banner
            try:
                accept_button = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'cc-btn') or contains(text(), 'Accetto')]"))
                )
                accept_button.click()
                time.sleep(2)
            except Exception:
                pass

            # Outcome mapping based on sport
            # Map sport-specific outcome keys based on configured market_map
            market_map = self.sports_config.get(sport, {}).get('market_map', {})
            # Default for H2H (Tennis, Cricket, MMA, etc.)
            outcome_keys = ['odds_1', 'odds_2']
            if sport == 'soccer':
                outcome_keys = ['odds_1', 'odds_x', 'odds_2']
            elif '1x2' in market_map and market_map['1x2'] == 'Full Time Result':
                 outcome_keys = ['odds_1', 'odds_x', 'odds_2']

            # Check for error page
            if "www.oddsportal.com" == self.driver.title or "Access Denied" in self.driver.page_source:
                logger.warning(f"Likely blocked or redirected on {match_url}")
                return []

            # Find all potential rows
            rows = self.driver.find_elements(By.CSS_SELECTOR, "div.flex.h-9.border-b.border-l.border-r.border-black-borders.text-xs")

            if not rows:
                rows = self.driver.find_elements(By.XPATH, "//div[.//div[contains(@class, 'odds-cell')]]")
                rows = [r for r in rows if len(r.find_elements(By.CSS_SELECTOR, ".odds-cell")) > 1]

            if not rows:
                logger.info(f"No odds rows found for {match_url} - likely no markets available currently.")
                return []
            logger.info(f"Found {len(rows)} odds row containers")

            opportunities = []
            for row in rows:
                try:
                    cells = row.find_elements(By.CSS_SELECTOR, ".odds-cell")
                    bookmaker_elements = row.find_elements(By.CSS_SELECTOR, "a.bookmakerImg")
                    bookmaker = "Unknown"
                    if bookmaker_elements:
                        href = bookmaker_elements[0].get_attribute('href')
                        if href:
                            match = re.search(r'/bookmaker/([^/]+)/', href)
                            if match:
                                bookmaker = match.group(1)

                    if len(cells) >= len(outcome_keys):
                        odds_values = [cell.text.strip() for cell in cells if cell.text.strip()]

                        if len(odds_values) >= len(outcome_keys):
                            opp = {'bookmaker': bookmaker}
                            for i, key in enumerate(outcome_keys):
                                opp[key] = odds_values[i]

                            opportunities.append(opp)
                except Exception as e:
                    logger.debug(f"Error parsing odds row: {e}")
                    continue

            logger.info(f"Extracted {len(opportunities)} structured odds rows")
            return opportunities

        except Exception as e:
            logger.error(f"Error scraping match {match_url}: {e}")
            return []

    def calculate_ev(self, soft_odds: float, all_odds: List[Dict], market_key: str = 'odds_1') -> Dict:
        """
        Calculate EV using Pinnacle as the baseline if available,
        otherwise use the market average adjusted for overround.
        """
        if not soft_odds or soft_odds <= 1.0:
            return {'ev': 0, 'ev_pct': 0}

        # 1. Find Pinnacle odds
        pinnacle_odds = None
        for opp in all_odds:
            if 'pinnacle' in opp.get('bookmaker', '').lower():
                pinnacle_odds = float(opp.get(market_key, 0))
                break

        if pinnacle_odds and pinnacle_odds > 1.0:
            baseline_odds = pinnacle_odds
        else:
            # 2. If no Pinnacle, use market average and adjust for overround
            valid_odds = []
            for opp in all_odds:
                try:
                    val = float(opp.get(market_key, 0))
                    if val > 1.0:
                        valid_odds.append(val)
                except (ValueError, TypeError):
                    continue

            if not valid_odds:
                return {'ev': 0, 'ev_pct': 0}

            # Calculate overround (sum of implied probabilities)
            implied_probs = [1 / o for o in valid_odds]
            overround = sum(implied_probs)

            # Average odds
            avg_odds = sum(valid_odds) / len(valid_odds)

            # Adjusted baseline (fair odds)
            # Fair probability = (1 / odds) / overround
            # Fair odds = 1 / ( (1/odds) / overround ) = odds * overround
            baseline_odds = avg_odds * overround

        ev = (soft_odds / baseline_odds) - 1
        return {'ev': ev, 'ev_pct': ev * 100}

    def _apply_delay(self):
        time.sleep(random.uniform(*self.delay_range))

    def _format_opportunity(self, ev_data: Dict, sport: str, event: str, market: str,
                           player: str, odds: float, sportsbook: str, game_date: str) -> Dict:
        return {
            'ev': ev_data['ev'],
            'sport': sport,
            'event': event,
            'market': market,
            'player': player,
            'odds': str(odds),
            'sportsbook': sportsbook,
            'game_date': game_date
        }

    def generate_simulated_opportunities(self) -> List[Dict]:
        logger.info("Generating simulated Oddsportal +EV opportunities")
        simulated = []

        # Mapping for generating realistic looking simulated data for different sports
        sport_examples = {
            'soccer': [("Arsenal vs Manchester City", "2026-05-17", "Premier League", 'Full Time Result')],
            'tennis': [("Federer vs Nadal", "2026-05-18", "ATP Wimbledon", 'Match Result')],
            'cricket': [("India vs Australia", "2026-05-19", "World Cup", 'Match Result')],
            'rugby': [("England vs France", "2026-05-20", "Six Nations", 'Match Result')],
            'mma': [("Fighter A vs Fighter B", "2026-05-21", "UFC", 'Match Result')]
        }

        for sport, matches in sport_examples.items():
            for event, date, league, market in matches:
                avg_odds = 2.0
                soft_odds = 2.15
                ev_data = self.calculate_ev(soft_odds, avg_odds)
                if ev_data['ev_pct'] >= self.min_ev * 100:
                    simulated.append(self._format_opportunity(
                        ev_data, sport, event, market, 'Home', soft_odds, 'Bet365', date
                    ))
        return simulated
