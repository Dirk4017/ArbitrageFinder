import subprocess
import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

class OddsHarvesterAdapter:
    def __init__(self, config_path: str = "configs/harvester_config.json"):
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")
        with open(config_path, 'r') as f:
            self.config = json.load(f)

    def scrape_sport(self, sport: str, leagues: List[str] = None, markets: List[str] = None) -> List[Dict]:
        """Calls OddsHarvester CLI and converts output."""
        opportunities = []

        # Build CLI command
        # Use full path to oddsharvester.exe
        cli_path = r"C:\Users\David\AppData\Local\Python\pythoncore-3.14-64\Scripts\oddsharvester.exe"
        cmd = [cli_path, "upcoming", "-s", sport, "-f", "json", "-d", datetime.now().strftime("%Y%m%d")]

        # Append league/market filters if provided
        if leagues:
            cmd.extend(["-l", ",".join(leagues)])
        if markets:
            cmd.extend(["-m", ",".join(markets)])

        # Add scraping options
        scraping = self.config.get('scraping', {})
        if scraping.get('headless'):
            cmd.append("--headless")
        else:
            cmd.append("--no-headless")

        if scraping.get('preview_mode'):
            cmd.append("--preview-only")
        else:
            cmd.append("--full-scrape")

        # Regional/Proxy settings
        if scraping.get('use_proxy') and scraping.get('proxy_url'):
            cmd.extend(["--proxy", scraping['proxy_url']])

        # Note: OddsHarvester is assumed to output JSON to stdout or can be configured to do so.
        # If it doesn't, this wrapper needs adjustment.
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            data = json.loads(result.stdout)

            # Map harvester output to our internal format
            for item in data:
                opportunities.append({
                    'ev': float(item.get('ev', 0)),
                    'sport': sport,
                    'event': f"{item.get('home_team')} vs {item.get('away_team')}",
                    'market': item.get('market'),
                    'player': item.get('player', 'N/A'),
                    'odds': item.get('odds'),
                    'sportsbook': item.get('bookmaker'),
                    'game_date': item.get('date'),
                    'league': item.get('league')
                })
        except subprocess.CalledProcessError as e:
            logger.error(f"OddsHarvester CLI error for {sport}: {e}")
            logger.error(f"STDOUT: {e.stdout}")
            logger.error(f"STDERR: {e.stderr}")
        except Exception as e:
            logger.error(f"Unexpected error for {sport}: {e}")

        return opportunities

    def scrape_all(self) -> List[Dict]:
        all_opps = []
        sports_cfg = self.config.get('sports', [])
        leagues_cfg = self.config.get('leagues', {})
        markets_cfg = self.config.get('markets', {})

        for sport in sports_cfg:
            all_opps.extend(self.scrape_sport(
                sport,
                leagues=leagues_cfg.get(sport),
                markets=markets_cfg.get(sport)
            ))
        return all_opps
