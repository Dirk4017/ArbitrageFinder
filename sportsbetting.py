# =============================================================================
# ENHANCED SPORTS BETTING PAPER TRADING SYSTEM
# WITH IDEMPOTENT R STATS RESOLUTION AND SUPABASE INTEGRATION
# =============================================================================

import pandas as pd
import numpy as np
import requests
import time
import json
import re
import os
import sys
import sqlite3
import logging
import uuid
import hashlib
import shlex
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple, Any
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import threading
from fuzzywuzzy import fuzz
import urllib3
import urllib.request
import urllib.parse
import urllib.error
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dataclasses import dataclass
from bs4 import BeautifulSoup
import subprocess
import platform

import os
import sys

# Fix Windows console encoding
if sys.platform == "win32":
    # Set console to UTF-8
    os.system('chcp 65001 > nul')

    # Force UTF-8 encoding for stdout/stderr
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

    # Alternative if reconfigure doesn't work:
    import io

    # Replace stdout/stderr with UTF-8 compatible versions
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='ignore')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='ignore')

    # Also fix the logging handler
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stdout
    )
# Disable SSL warnings for development
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Try to import Supabase, but make it optional
try:
    from supabase import create_client, Client

    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False
    print("Supabase not installed. Run: pip install supabase")

# Import the ESPN wrapper (optional)
try:
    from espn_wrapper import ESPNPlayerStats

    ESPN_WRAPPER_AVAILABLE = True
except ImportError:
    ESPN_WRAPPER_AVAILABLE = False
    print("ESPN wrapper not available. Install with: pip install requests pandas")


class ComprehensiveMarketResolver:
    """Comprehensive market resolver that handles ALL bet types"""

    def __init__(self, r_script_path=None):
        # Market categorization - EXPANDED FOR YOUR MARKETS
        self.PLAYER_STAT_MARKETS = {
            # NBA
            'player points': ('points', 'points'),
            'player rebounds': ('rebounds', 'rebounds'),
            'player assists': ('assists', 'assists'),
            'player steals': ('steals', 'steals'),
            'player blocks': ('blocks', 'blocks'),
            'player turnovers': ('turnovers', 'turnovers'),
            'player threes': ('three_pointers_made', 'three_pointers_made'),
            'player free throws': ('free_throws_made', 'free_throws_made'),
            'player field goals': ('field_goals_made', 'field_goals_made'),
            'player double double': ('double_double', 'boolean_positive'),

            # NFL
            'player passing yards': ('passing_yards', 'passing_yards'),
            'player passing touchdowns': ('passing_tds', 'passing_tds'),
            'player passing interceptions': ('passing_ints', 'passing_ints'),
            'player passing completions': ('completions', 'completions'),
            'player passing attempts': ('attempts', 'attempts'),
            'player rushing yards': ('rushing_yards', 'rushing_yards'),
            'player rushing touchdowns': ('rushing_tds', 'rushing_tds'),
            'player rushing attempts': ('rushing_attempts', 'rushing_attempts'),
            'player receiving yards': ('receiving_yards', 'receiving_yards'),
            'player receiving touchdowns': ('receiving_tds', 'receiving_tds'),
            'player receptions': ('receptions', 'receptions'),
            'player targets': ('targets', 'targets'),

            # Football (generic)
            'player touchdowns': ('total_tds', 'touchdowns'),
            'player yards': ('total_yards', 'yards'),

            # CRAZYNINJA SPECIFIC
            'player first touchdown scorer': ('first_scorer', 'equals_player'),
            'player last touchdown scorer': ('last_scorer', 'equals_player'),
            'player anytime touchdown scorer': ('touchdowns', 'boolean_positive'),
            'player to score a touchdown': ('touchdowns', 'boolean_positive'),
            'player to score first': ('first_scorer', 'equals_player'),
            'player to score last': ('last_scorer', 'equals_player'),
            'team player first field goal': ('first_scorer', 'equals_player'),
            'player first basket': ('first_scorer', 'equals_player'),
            'player first field goal': ('first_scorer', 'equals_player'),
        }

        self.OVER_UNDER_MARKETS = {
            'player over': ('points', 'over_under'),
            'player under': ('points', 'over_under'),
            'player receptions over': ('receptions', 'over_under'),
            'player receptions under': ('receptions', 'over_under'),
            'player receiving yards over': ('receiving_yards', 'over_under'),
            'player receiving yards under': ('receiving_yards', 'over_under'),
            'player rushing yards over': ('rushing_yards', 'over_under'),
            'player rushing yards under': ('rushing_yards', 'over_under'),
            'player passing yards over': ('passing_yards', 'over_under'),
            'player passing yards under': ('passing_yards', 'over_under'),
            'player touchdowns over': ('touchdowns', 'over_under'),
            'player touchdowns under': ('touchdowns', 'over_under'),
            # NEW: ADD THESE FOR NBA
            'player threes over': ('three_pointers_made', 'over_under'),
            'player threes under': ('three_pointers_made', 'over_under'),
            'player blocks over': ('blocks', 'over_under'),
            'player blocks under': ('blocks', 'over_under'),
            'player steals over': ('steals', 'over_under'),
            'player steals under': ('steals', 'over_under'),
            'player rebounds over': ('rebounds', 'over_under'),
            'player rebounds under': ('rebounds', 'over_under'),
            'player assists over': ('assists', 'over_under'),
            'player assists under': ('assists', 'over_under'),
            'player points over': ('points', 'over_under'),
            'player points under': ('points', 'over_under'),
            'player points + assists over': ('points_assists', 'over_under'),
            'player points + assists under': ('points_assists', 'over_under'),
            'player rebounds + assists over': ('rebounds_assists', 'over_under'),
            'player rebounds + assists under': ('rebounds_assists', 'over_under'),
        }

        self.GAME_MARKETS = {
            # Moneyline
            'moneyline': ('moneyline', 'team_win'),

            # Totals
            'total points': ('total_points', 'total_over_under'),
            'over': ('total_points', 'total_over_under'),
            'under': ('total_points', 'total_over_under'),
            '1st half total points': ('total_points', 'total_over_under'),

            # Spread
            'point spread': ('spread', 'spread'),
            'spread': ('spread', 'spread'),
        }

    def resolve_market(self, r_result: Dict, bet: Dict, line_value: Optional[float] = None) -> Tuple[bool, float]:
        """Main resolution function that handles ALL market types"""
        try:
            market = bet.get('market', '').lower()
            sport = bet.get('sport', '').lower()
            player = bet.get('player', '')

            # Extract over/under direction from market or player name
            direction = self._extract_direction(market, player)

            # Clean market name (remove direction for matching)
            clean_market = self._clean_market_name(market, direction)

            # Get resolution method
            resolution_method = self._get_resolution_method(clean_market, sport)

            if resolution_method:
                won, actual_value = resolution_method(r_result, bet, clean_market, line_value, direction)
                return won, actual_value
            else:
                # Fallback to standard player prop
                return self._resolve_fallback(r_result, bet, line_value, direction)

        except Exception as e:
            raise Exception(f"Market resolution error: {str(e)}")

    def _get_resolution_method(self, market: str, sport: str):
        """Get appropriate resolution method"""
        # Check for first/last scorer markets
        if any(keyword in market for keyword in ['first', 'last', 'scorer', 'field goal', 'basket', 'td']):
            return self._resolve_scorer_market

        # Check for over/under markets
        if 'over' in market or 'under' in market:
            return self._resolve_over_under_market

        # Check for player stat markets
        for pattern, (stat_key, _) in self.PLAYER_STAT_MARKETS.items():
            if pattern in market:
                return self._resolve_player_stat_market

        # Check for game markets
        for pattern, (stat_key, _) in self.GAME_MARKETS.items():
            if pattern in market:
                return self._resolve_game_market

        return None

    def _resolve_scorer_market(self, r_result: Dict, bet: Dict, market: str,
                               line_value: Optional[float], direction: str) -> Tuple[bool, float]:
        """Resolve first/last scorer markets"""
        player = bet.get('player', '')
        data = r_result.get('data', {})

        # Extract just the player name (remove team name if present)
        player_clean = self._extract_player_name_from_bet(player)

        # Get scorer from R result
        if 'first' in market:
            scorer = data.get('first_scorer', '')
        elif 'last' in market:
            scorer = data.get('last_scorer', '')
        else:
            scorer = ''

        # Check if player matches scorer
        won = self._player_name_matches(player_clean, scorer)
        actual_value = 1 if won else 0

        return won, actual_value

    def _resolve_over_under_market(self, r_result: Dict, bet: Dict, market: str,
                                   line_value: Optional[float], direction: str) -> Tuple[bool, float]:
        """Resolve over/under markets"""
        player = bet.get('player', '')
        data = r_result.get('data', {})
        stats = data.get('stats', {})

        # Determine which stat to use
        stat_key = None
        if 'receptions' in market:
            stat_key = 'receptions'
        elif 'receiving yards' in market:
            stat_key = 'receiving_yards'
        elif 'rushing yards' in market:
            stat_key = 'rushing_yards'
        elif 'passing yards' in market:
            stat_key = 'passing_yards'
        elif 'touchdowns' in market:
            stat_key = 'touchdowns'
        elif 'threes' in market or 'three' in market:
            stat_key = 'three_pointers_made'
        elif 'blocks' in market:
            stat_key = 'blocks'
        elif 'steals' in market:
            stat_key = 'steals'
        elif 'rebounds' in market:
            stat_key = 'rebounds'
        elif 'assists' in market:
            stat_key = 'assists'
        elif 'points' in market:
            stat_key = 'points'
        elif 'points + assists' in market:
            # Calculate combined stat
            points = stats.get('points', 0)
            assists = stats.get('assists', 0)
            actual_value = points + assists
        elif 'rebounds + assists' in market:
            # Calculate combined stat
            rebounds = stats.get('rebounds', 0)
            assists = stats.get('assists', 0)
            actual_value = rebounds + assists
        else:
            # Default to points
            stat_key = 'points'

        if stat_key:
            actual_value = stats.get(stat_key, 0)

        if direction == 'over':
            won = actual_value > line_value if line_value is not None else actual_value > 0
        elif direction == 'under':
            won = actual_value < line_value if line_value is not None else actual_value < 0
        else:
            # Default to over
            won = actual_value > line_value if line_value is not None else actual_value > 0

        return won, actual_value

    def _resolve_player_stat_market(self, r_result: Dict, bet: Dict, market: str,
                                    line_value: Optional[float], direction: str) -> Tuple[bool, float]:
        """Resolve standard player stat markets"""
        player = bet.get('player', '')
        data = r_result.get('data', {})
        stats = data.get('stats', {})

        # Find the stat key
        stat_key = None
        for pattern, (key, _) in self.PLAYER_STAT_MARKETS.items():
            if pattern in market:
                stat_key = key
                break

        if not stat_key:
            # Default to points
            stat_key = 'points'

        actual_value = stats.get(stat_key, 0)

        # For Yes/No markets (like "player to score a touchdown")
        if 'score' in market or 'scorer' in market or 'touchdown' in market or 'double double' in market:
            won = actual_value > 0 if stat_key != 'double_double' else actual_value
        elif line_value is not None:
            # Over/under comparison
            if direction == 'over':
                won = actual_value > line_value
            elif direction == 'under':
                won = actual_value < line_value
            else:
                won = actual_value > line_value
        else:
            # Boolean check (did they have any?)
            won = actual_value > 0

        return won, actual_value

    def _resolve_game_market(self, r_result: Dict, bet: Dict, market: str,
                             line_value: Optional[float], direction: str) -> Tuple[bool, float]:
        """Resolve game-level markets (moneyline, totals, spread)"""
        data = r_result.get('data', {})
        event = bet.get('event', '')
        player_or_team = bet.get('player', '')

        if 'moneyline' in market:
            # Team win market
            home_team = data.get('home_team', '')
            away_team = data.get('away_team', '')
            home_score = data.get('home_score', 0)
            away_score = data.get('away_score', 0)

            # Determine which team was bet on
            if self._team_name_matches(player_or_team, home_team) or 'home' in player_or_team.lower():
                won = home_score > away_score
                actual_value = home_score - away_score
            elif self._team_name_matches(player_or_team, away_team) or 'away' in player_or_team.lower():
                won = away_score > home_score
                actual_value = away_score - home_score
            else:
                raise Exception(f"Could not determine team for moneyline: {player_or_team}")

        elif 'total' in market or 'over' in market or 'under' in market:
            # Total points market
            home_score = data.get('home_score', 0)
            away_score = data.get('away_score', 0)
            total = home_score + away_score

            if direction == 'over':
                won = total > line_value if line_value is not None else total > 0
            elif direction == 'under':
                won = total < line_value if line_value is not None else total < 0
            else:
                won = total > line_value if line_value is not None else total > 0

            actual_value = total

        elif 'spread' in market:
            # Point spread market
            home_team = data.get('home_team', '')
            away_team = data.get('away_team', '')
            home_score = data.get('home_score', 0)
            away_score = data.get('away_score', 0)

            if self._team_name_matches(player_or_team, home_team):
                adjusted_home_score = home_score + (line_value or 0)
                won = adjusted_home_score > away_score
                actual_value = adjusted_home_score - away_score
            elif self._team_name_matches(player_or_team, away_team):
                adjusted_away_score = away_score + (line_value or 0)
                won = adjusted_away_score > home_score
                actual_value = adjusted_away_score - home_score
            else:
                raise Exception(f"Could not determine team for spread: {player_or_team}")

        else:
            raise Exception(f"Unknown game market: {market}")

        return won, actual_value

    def _resolve_fallback(self, r_result: Dict, bet: Dict, line_value: Optional[float],
                          direction: str) -> Tuple[bool, float]:
        """Fallback resolution for unknown markets"""
        # Try to extract stat from market name
        market = bet.get('market', '').lower()
        data = r_result.get('data', {})
        stats = data.get('stats', {})

        # Try to guess which stat
        if 'yards' in market:
            if 'receiving' in market:
                stat_key = 'receiving_yards'
            elif 'rushing' in market:
                stat_key = 'rushing_yards'
            elif 'passing' in market:
                stat_key = 'passing_yards'
            else:
                stat_key = 'total_yards'
        elif 'touchdown' in market or 'td' in market:
            stat_key = 'touchdowns'
        elif 'three' in market or '3pt' in market or 'threes' in market:
            stat_key = 'three_pointers_made'
        elif 'reception' in market:
            stat_key = 'receptions'
        elif 'points' in market:
            stat_key = 'points'
        elif 'rebounds' in market:
            stat_key = 'rebounds'
        elif 'assists' in market:
            stat_key = 'assists'
        elif 'steals' in market:
            stat_key = 'steals'
        elif 'blocks' in market:
            stat_key = 'blocks'
        elif 'turnovers' in market:
            stat_key = 'turnovers'
        else:
            stat_key = 'points'  # Default

        actual_value = stats.get(stat_key, 0)

        # Determine win/loss
        if direction == 'over':
            won = actual_value > line_value if line_value is not None else actual_value > 0
        elif direction == 'under':
            won = actual_value < line_value if line_value is not None else actual_value < 0
        else:
            # Boolean check
            won = actual_value > (line_value or 0.5)

        return won, actual_value

    def _extract_player_name_from_bet(self, player_string: str) -> str:
        """Extract just the player name from bet string"""
        import re

        # Remove team names and over/under indicators
        player_clean = player_string

        # Remove common team prefixes
        team_prefixes = [
            'Portland Trail Blazers', 'Oklahoma City Thunder', 'Brooklyn Nets',
            'Chicago Bulls', 'Cleveland Cavaliers', 'Philadelphia 76ers',
            'Los Angeles Clippers', 'Toronto Raptors', 'Detroit Pistons',
            'Boston Celtics', 'Minnesota Timberwolves', 'Phoenix Suns',
            'Utah Jazz', 'Houston Rockets', 'Denver Broncos', 'Buffalo Bills',
            'New England Patriots', 'Houston Texans', 'Miami (FL)', 'Indiana',
            'Miami Dolphins', 'Kansas City Chiefs', 'Baltimore Ravens',
            'San Francisco 49ers', 'Green Bay Packers', 'Dallas Cowboys'
        ]

        for prefix in team_prefixes:
            if player_clean.startswith(prefix):
                player_clean = player_clean.replace(prefix, '').strip()
                break

        # Remove Yes/No suffixes
        if player_clean.endswith('Yes'):
            player_clean = player_clean[:-3].strip()
        elif player_clean.endswith('No'):
            player_clean = player_clean[:-2].strip()

        # Remove Over/Under with numbers
        player_clean = re.sub(r'\s+[Oo]ver\s+\d+\.?\d*\s*$', '', player_clean)
        player_clean = re.sub(r'\s+[Uu]nder\s+\d+\.?\d*\s*$', '', player_clean)
        player_clean = re.sub(r'\s+-\s*\d+\.?\d*\s*$', '', player_clean)
        player_clean = re.sub(r'\s+\d+\.?\d*\s*$', '', player_clean)

        return player_clean.strip()

    def _extract_direction(self, market: str, player: str) -> str:
        """Extract over/under direction from market or player name"""
        market_lower = market.lower()
        player_lower = player.lower()

        if 'over' in market_lower or 'over' in player_lower or ' o ' in player_lower:
            return 'over'
        elif 'under' in market_lower or 'under' in player_lower or ' u ' in player_lower:
            return 'under'
        return ''

    def _clean_market_name(self, market: str, direction: str) -> str:
        """Remove direction and clean up market name"""
        clean = market.lower()
        if direction:
            clean = clean.replace(direction, '').strip()
        clean = clean.replace('over', '').replace('under', '').strip()
        return clean

    def _player_name_matches(self, name1: str, name2: str) -> bool:
        """Check if two player names match"""
        if not name1 or not name2:
            return False

        # Simple matching
        clean1 = name1.lower().replace('.', '').replace('jr', '').replace('sr', '').strip()
        clean2 = name2.lower().replace('.', '').replace('jr', '').replace('sr', '').strip()

        return (clean1 == clean2 or
                clean1 in clean2 or
                clean2 in clean1 or
                clean1.split()[-1] == clean2.split()[-1])

    def _team_name_matches(self, name1: str, name2: str) -> bool:
        """Check if two team names match"""
        if not name1 or not name2:
            return False

        clean1 = name1.lower()
        clean2 = name2.lower()

        # Common team mappings
        team_aliases = {
            'jets': ['ny jets', 'new york jets'],
            'dolphins': ['miami dolphins'],
            'chiefs': ['kansas city chiefs'],
            'ravens': ['baltimore ravens'],
            'lakers': ['la lakers', 'los angeles lakers'],
            'warriors': ['golden state warriors'],
            'celtics': ['boston celtics'],
        }

        if clean1 == clean2:
            return True

        # Check aliases
        for base, aliases in team_aliases.items():
            if clean1 == base and clean2 in aliases:
                return True
            if clean2 == base and clean1 in aliases:
                return True

        # Partial match
        return (clean1 in clean2 or clean2 in clean1)

# =============================================================================
# PRODUCTION-GRADE R STATS RESOLVER
# =============================================================================
class RStatsResolver:
    """
    Production-grade bet resolver using R (nflverse/hoopR)
    Python is thin orchestrator, R is source of truth for all game data
    """

    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.logger = logging.getLogger(__name__)
        self.is_windows = platform.system() == "Windows"

        # R configuration
        self.rscript_path = self._find_rscript()
        if not self.rscript_path:
            raise RuntimeError("Rscript not found. Install R from https://cran.r-project.org/")

        self.r_script_path = "bet_resolver.R"
        if not os.path.exists(self.r_script_path):
            raise FileNotFoundError(f"R script not found: {self.r_script_path}")

        # Tracking and state
        self.r_calls = 0
        self.r_successful = 0
        self.resolution_attempts: Dict[str, Dict] = {}  # bet_key -> {attempts, last_attempt, last_error}
        self.max_attempts_per_bet = 5
        self.retry_cooldown_hours = 12

        # Cache system
        self.cache_db = self._init_cache()

        # Sport-specific configuration
        self.sport_configs = {
            'nfl': {'season_start_month': 9, 'season_end_month': 2},
            'nba': {'season_start_month': 10, 'season_end_month': 6},
        }

        self.logger.info(f"RStatsResolver initialized (Rscript: {self.rscript_path})")

    # ------------------------------------------------------------------
    # 1. RESOLUTION GATING (Conservative but correct)
    # ------------------------------------------------------------------
    def _should_resolve_bet(self, bet: Dict) -> bool:
        """Debug version to see what's happening"""
        player = bet.get("player") or bet.get("player_name", "")
        game_date = bet.get('game_date')

        self.logger.info(f"DEBUG _should_resolve_bet called for: {player}")
        self.logger.info(f"DEBUG Game date in bet: {game_date}")
        self.logger.info(f"DEBUG Bet keys: {list(bet.keys())}")

        # TEMPORARY: Always return True for testing R
        self.logger.info("DEBUG MODE: Allowing all bets for R testing")
        return True

    # ------------------------------------------------------------------
    # 2. MAIN RESOLUTION ENTRY POINT
    # ------------------------------------------------------------------
    def _clean_player_and_extract_line(self, player_str: str, market_str: str = "") -> Tuple[
        str, Optional[float], Optional[str]]:
        """
        Clean player names that contain line values and extract line value.
        Returns: (cleaned_player_name, line_value, over_under_direction)

        Enhanced: Also check market string for direction keywords
        """
        import re

        player_original = player_str
        line_value = None
        direction = None

        # Store original for debugging
        original_for_debug = player_str

        # Common patterns with direction and number
        patterns = [
            # Full patterns with "over" or "under"
            (r'\s+[Oo]ver\s+(\d+\.?\d*)\s*$', 1, "over"),  # " Over 4.5"
            (r'\s+[Uu]nder\s+(\d+\.?\d*)\s*$', 1, "under"),  # " Under 0.5"
            (r'\s+O\s+(\d+\.?\d*)\s*$', 1, "over"),  # " O 4.5"
            (r'\s+U\s+(\d+\.?\d*)\s*$', 1, "under"),  # " U 0.5"
            (r'\s+-\s*(\d+\.?\d*)\s*$', 1, "under"),  # " -4.5" (usually means under)
            (r'\s+\+\s*(\d+\.?\d*)\s*$', 1, "over"),  # " +4.5" (usually means over)
            # Just numbers at the end (no direction specified)
            (r'\s+(\d+\.?\d*)\s*$', 1, None),  # " 4.5" at the end
            # Parentheses with numbers
            (r'\s*\((\d+\.?\d*)\)\s*$', 1, None),  # " (4.5)"
        ]

        cleaned = player_str
        matched = False

        for pattern, group_num, pattern_direction in patterns:
            match = re.search(pattern, cleaned)
            if match:
                try:
                    # Extract line value
                    line_value = float(match.group(group_num))

                    # Use pattern direction if available
                    if pattern_direction:
                        direction = pattern_direction

                    # Remove the matched pattern from player name
                    cleaned = re.sub(pattern, '', cleaned)
                    matched = True
                    print(f"[DEBUG] Pattern '{pattern}' matched, extracted {line_value}, direction: {direction}")
                    break  # Stop after first match
                except (ValueError, IndexError) as e:
                    print(f"[DEBUG] Pattern match failed: {e}")
                    continue

        # If no pattern matched but string contains "over" or "under", extract direction
        if not matched:
            if re.search(r'\b[Oo]ver\b', player_original):
                direction = "over"
                # Remove "over" from the name
                cleaned = re.sub(r'\s+[Oo]ver\b', '', cleaned)
                print(f"[DEBUG] Found 'over' in player name, direction: {direction}")
            elif re.search(r'\b[Uu]nder\b', player_original):
                direction = "under"
                # Remove "under" from the name
                cleaned = re.sub(r'\s+[Uu]nder\b', '', cleaned)
                print(f"[DEBUG] Found 'under' in player name, direction: {direction}")

        # ========== NEW: Check market string for direction keywords ==========
        if not direction and market_str:
            market_lower = market_str.lower()
            # Look for "over" or "under" in market description
            if "over" in market_lower:
                direction = "over"
                print(f"[DEBUG] Found 'over' in market string, direction: {direction}")
            elif "under" in market_lower:
                direction = "under"
                print(f"[DEBUG] Found 'under' in market string, direction: {direction}")
            # Also check for "O/U" pattern
            elif "o/u" in market_lower or "o / u" in market_lower:
                print(f"[DEBUG] Found 'O/U' in market, direction should come from player string")
                # For "Player Passing Yards O/U", direction should come from player string
                pass

        # Clean up any extra whitespace
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()

        # Remove any trailing special characters
        cleaned = re.sub(r'[.,;:\s]+$', '', cleaned)

        # Debug output
        if cleaned != player_original:
            print(f"[DEBUG] Player name cleaned: '{player_original}' → '{cleaned}'")

        return cleaned, line_value, direction

    def _extract_line_from_market(self, market: str) -> Optional[float]:
        """Extract line value from market string"""
        import re

        try:
            # Look for numbers with optional decimal point
            matches = re.findall(r'(\d+\.?\d*)', market)
            if matches:
                # Return the last number found (most likely the line value)
                return float(matches[-1])
        except (ValueError, TypeError):
            pass

        return None

    def _is_future_game(self, game_date: str) -> bool:
        """Check if a game date is in the future - FIXED VERSION"""
        if not game_date:
            return False

        try:
            # Parse the game date
            game_date_obj = datetime.strptime(str(game_date), '%Y-%m-%d').date()
            current_date = datetime.now().date()

            print(f"[DEBUG DATE CHECK] Game: {game_date_obj}, Current: {current_date}")

            # If game is in the future (more than 1 day ahead)
            if game_date_obj > current_date:
                print(f"[DEBUG] Game {game_date} IS in the future")
                return True

            # If game is today or yesterday, we might need to wait
            if game_date_obj == current_date:
                current_hour = datetime.now().hour
                # Most NFL/NBA games are in the evening, so wait until after 9 PM
                if current_hour < 21:  # 9 PM
                    print(f"[DEBUG] Game {game_date} is today, but before 9 PM")
                    return True

            # Game is in the past - should be resolvable!
            print(f"[DEBUG] Game {game_date} is in the PAST - should be resolvable")
            return False

        except Exception as e:
            print(f"Could not parse game date '{game_date}': {e}")
            return False

    def resolve_player_prop(self, bet: Dict) -> Tuple[bool, float]:
        """
        Main resolution with proper retry tracking and R as source of truth
        """
        try:
            # ========== ADD COMPREHENSIVE DEBUG ==========
            print(f"\n[DEBUG resolve_player_prop] Bet ID: {bet.get('id')}")

            # ========== CRITICAL FIX: Use ACTUAL DATABASE COLUMN NAMES ==========
            player_raw = bet.get("player", "")  # ← DATABASE COLUMN: 'player'
            sport = bet.get("sport", "").lower()
            market_raw = bet.get("market", "")  # ← DATABASE COLUMN: 'market'
            event = bet.get("event", "")  # ← DATABASE COLUMN: 'event'
            stake = float(bet.get("stake", 0))
            odds = float(bet.get("odds", 0))
            game_date = bet.get('game_date')

            # ========== CHECK CURRENT DATE FIRST ==========
            current_date = datetime.now().date()
            print(f"[DEBUG] Current system date: {current_date}")
            print(f"[DEBUG] Game date from bet: {game_date}")

            if game_date:
                try:
                    game_date_obj = datetime.strptime(str(game_date), '%Y-%m-%d').date()
                    print(f"[DEBUG] Game date parsed: {game_date_obj}")

                    # Show what's really happening
                    if game_date_obj > current_date:
                        print(f"⚠️  WARNING: Game date {game_date} is AFTER current date {current_date}")
                        print(f"   This suggests either:")
                        print(f"   1. System date is wrong (it's showing 2026 but should be 2025?)")
                        print(f"   2. Game really is in the future")
                    else:
                        print(f"✓ Game date {game_date} is in the PAST relative to {current_date}")
                except Exception as e:
                    print(f"[ERROR] Could not parse game date: {e}")

            # ========== QUICK FIX 1: SKIP COLLEGE GAMES ==========
            event_lower = event.lower()
            if any(college in event_lower for college in ['oregon', 'texas tech', 'college', 'ncaa']):
                print(f"\n❌ COLLEGE GAME DETECTED: {event}")
                print(f"   Sport: {sport}, Market: {market_raw}, Player: {player_raw}")
                print(f"   College games are not supported by the R resolver.")
                raise Exception(f"College (NCAA) game detected: {event}. Only NFL/NBA games are supported.")

            # ========== FIXED: Check if game is REALLY in future ==========
            if game_date and self._is_future_game(game_date):
                print(f"\n⏭️  GAME IS IN FUTURE OR TODAY BEFORE 9 PM: {game_date}")
                print(f"   Current date: {current_date}")
                print(f"   Cannot resolve games that haven't happened yet.")
                raise Exception(f"Game is in the future or hasn't completed yet ({game_date}). Cannot resolve yet.")

            # ========== ENHANCED: CLEAN PLAYER NAME AND EXTRACT LINE VALUE ==========
            # ... rest of your existing code continues from here ...
            # ========== ENHANCED: CLEAN PLAYER NAME AND EXTRACT LINE VALUE ==========
            player_clean, line_value_from_player, over_under_direction = self._clean_player_and_extract_line(
                player_raw, market_raw
            )
            # Use cleaned player name for all operations
            player = player_clean

            # DEBUG the cleaning process
            if player_raw != player:
                print(f"[DEBUG] Cleaned player name: '{player_raw}' → '{player}'")
                if line_value_from_player is not None:
                    print(f"[DEBUG] Extracted line value from player name: {line_value_from_player}")
                if over_under_direction:
                    print(f"[DEBUG] Detected direction from player name: {over_under_direction}")

            # Also extract line value from market string
            line_value_from_market = self._extract_line_from_market(market_raw)

            # Determine which line value to use (priority: player > market)
            line_value = line_value_from_player if line_value_from_player is not None else line_value_from_market

            # DEBUG: Log what we extracted (SHOW REALITY)
            print(f"\n[DEBUG] EXTRACTED FROM DATABASE (ACTUAL COLUMNS):")
            print(f"  player (raw): '{player_raw}' → cleaned: '{player}'")
            print(f"  sport: '{sport}'")
            print(f"  market (from 'market' column): '{market_raw}'")
            print(f"  event (from 'event' column): '{event}'")
            print(f"  game_date: '{game_date}'")
            print(f"  stake: {stake}, odds: {odds}")
            print(f"  line_value (from player): {line_value_from_player}")
            print(f"  line_value (from market): {line_value_from_market}")
            print(f"  final line_value: {line_value}")
            print(f"  detected direction: {over_under_direction}")

            # Check if we got empty values
            if not player:
                print(f"[ERROR] Player name is EMPTY! Check database column 'player'")
            if not market_raw:
                print(f"[ERROR] Market is EMPTY! Check database column 'market'")
            if not event:
                print(f"[ERROR] Event is EMPTY! Check database column 'event'")
            if not game_date:
                print(f"[WARNING] Game date is EMPTY! Check database column 'game_date'")

            # Update retry tracking (use original player for tracking)
            bet_key = self._get_bet_key(bet)
            if bet_key not in self.resolution_attempts:
                self.resolution_attempts[bet_key] = {
                    'attempts': 0,
                    'last_attempt': None,
                    'last_error': None,
                    'player': player_raw,  # Store original
                    'event': event,
                    'game_date': game_date
                }

            attempt_info = self.resolution_attempts[bet_key]
            attempt_info['attempts'] += 1
            attempt_info['last_attempt'] = datetime.now()

            self.logger.info(f"Resolution attempt #{attempt_info['attempts']} for {player} - {market_raw}")

            # Gate check
            if not self._should_resolve_bet(bet):
                raise Exception("Resolution not yet appropriate")

            # Determine market type - FIXED VERSION
            market_lower = market_raw.lower()
            print(f"[DEBUG] Market string: '{market_raw}'")
            print(f"[DEBUG] Market lower: '{market_lower}'")

            # Check for special markets (first/last scorer)
            if any(keyword in market_lower for keyword in ["first", "last", "scorer"]):
                market_type = self._determine_special_market_type(market_raw)
                print(f"[DEBUG] Special market detected: '{market_raw}' -> '{market_type}'")
            else:
                # ========== CRITICAL FIX: Build proper market type with direction ==========
                if over_under_direction and "over" not in market_lower and "under" not in market_lower:
                    # Construct market type based on market content and direction
                    if "reception" in market_lower:
                        market_type = f"player receptions {over_under_direction}"
                    elif "touchdown" in market_lower or "td" in market_lower:
                        if "passing" in market_lower and "td" in market_lower:
                            market_type = f"player passing touchdowns {over_under_direction}"
                        else:
                            market_type = f"player touchdowns {over_under_direction}"
                    elif "passing" in market_lower:
                        if "yards" in market_lower:
                            market_type = f"player passing yards {over_under_direction}"
                        elif "td" in market_lower:
                            market_type = f"player passing touchdowns {over_under_direction}"
                        else:
                            market_type = f"player passing {over_under_direction}"
                    elif "yards" in market_lower:
                        if "receiving" in market_lower:
                            market_type = f"player receiving yards {over_under_direction}"
                        elif "rushing" in market_lower:
                            market_type = f"player rushing yards {over_under_direction}"
                        else:
                            market_type = f"player yards {over_under_direction}"
                    else:
                        # Generic market type with direction
                        market_type = f"{market_raw} {over_under_direction}"
                    print(f"[DEBUG] Added direction to market type: '{market_type}'")
                else:
                    # If direction already in market or we don't have one, use market as-is
                    market_type = market_raw
                    print(f"[DEBUG] Using raw market type: '{market_type}'")

            # ========== EXTRACT SEASON FROM GAME_DATE ==========
            season = self._extract_season_hint(event, sport, game_date)

            # DEBUG season calculation
            print(f"[DEBUG] Season calculation:")
            print(f"  game_date: '{game_date}'")
            print(f"  calculated season: {season}")

            # ========== NORMALIZE SPORT FOR R SCRIPT ==========
            sport_for_r = sport.lower()
            if sport_for_r == "football":
                sport_for_r = "nfl"
            elif sport_for_r == "basketball":
                sport_for_r = "nba"

            print(f"[DEBUG] Sport normalized for R: '{sport}' -> '{sport_for_r}'")

            # ========== CHECK FOR COLLEGE FOOTBALL (ENHANCED) ==========
            if sport_for_r == "nfl" and self._is_college_game(event, sport):
                print(f"\n❌ COLLEGE FOOTBALL DETECTED: '{event}'")
                print(f"   Market: {market_raw}, Player: {player}")
                print(f"   The R resolver only supports NFL (professional) games.")
                raise Exception(f"College football game detected: {event}. Only professional NFL games are supported.")

            # ========== CHECK IF GAME IS IN FUTURE (ENHANCED) ==========
            if game_date and self._is_future_game(game_date):
                print(f"\n⏭️  GAME IS IN FUTURE: {game_date}")
                print(f"   Current date: {datetime.now().strftime('%Y-%m-%d')}")
                print(f"   Cannot resolve games that haven't happened yet.")
                raise Exception(f"Game is in the future ({game_date}). Cannot resolve yet.")

            print(f"\n[DEBUG] FINAL PARAMETERS FOR R SCRIPT:")
            print(f"  player_name='{player}' (cleaned)")
            print(f"  sport='{sport_for_r}' (normalized)")
            print(f"  season={season} (calculated from game_date)")
            print(f"  market_type='{market_type}' (converted from: '{market_raw}')")
            print(f"  event_string='{event}'")
            print(f"  line_value={line_value} (extracted from player/market)")
            print(f"  game_date='{game_date}'")

            # ========== Call R with 7 parameters ==========
            r_result = self._call_r_script(
                player_name=player,  # Use CLEANED player name
                sport=sport_for_r,  # Normalized sport (nfl/nba)
                season=season,
                market_type=market_type,  # Now includes direction if needed
                event_string=event,
                line_value=line_value,
                game_date=game_date
            )

            # DEBUG: Show R result
            print(f"\n[DEBUG] R SCRIPT RESULT:")
            print(f"  success: {r_result.get('success')}")
            print(f"  resolved: {r_result.get('resolved')}")
            print(f"  error: {r_result.get('error')}")
            if r_result.get('game_id'):
                print(f"  game_id: {r_result.get('game_id')}")
            if r_result.get('resolved') and r_result.get('data'):
                print(f"  data keys: {list(r_result['data'].keys())}")

            # Check if R returned a college game error
            if not r_result.get("success") and "college" in r_result.get("error", "").lower():
                print(f"\n❌ R SCRIPT IDENTIFIED COLLEGE GAME: {r_result.get('error')}")
                raise Exception(f"College game detected by R script: {event}")

            # --------------------------------------------------------
            # SIMPLIFIED CONTRACT WITH R
            # --------------------------------------------------------

            # Level 1: Did R script execute successfully?
            if not r_result.get("success", False):
                error_msg = r_result.get("error", "R execution failed")
                attempt_info['last_error'] = error_msg
                print(f"[ERROR] R script failed: {error_msg}")

                # Check if this is a retryable error
                if self._is_retryable_error(error_msg):
                    raise Exception(f"Retryable error: {error_msg}")
                else:
                    # Permanent failure
                    raise Exception(f"R script failed: {error_msg}")

            # Level 2: Did R resolve the bet?
            if not r_result.get("resolved", False):
                reason = r_result.get("reason", "Game not yet resolved")
                attempt_info['last_error'] = reason
                print(f"[ERROR] Game not resolved: {reason}")

                # This is expected - game exists but isn't complete
                raise Exception(f"Game not resolved: {reason}")

            # --------------------------------------------------------
            # BET SUCCESSFULLY RESOLVED
            # --------------------------------------------------------

            # Clear retry tracking
            self.resolution_attempts.pop(bet_key, None)

            # ========== USE COMPREHENSIVE RESOLUTION LOGIC ==========
            stake = float(bet.get('stake', 0))
            odds = float(bet.get('odds', 0))

            # Handle all market types comprehensively
            won, profit = self._resolve_comprehensive_market(
                bet, r_result, line_value, over_under_direction
            )

            self.logger.info(f"{'WIN' if won else 'LOSS'}: {player} - €{profit:+.2f}")
            print(f"[SUCCESS] Bet resolved: won={won}, profit={profit}")

            return won, profit

        except Exception as e:
            self.logger.warning(f"Resolution failed: {e}")
            print(f"[EXCEPTION] resolve_player_prop failed: {e}")
            import traceback
            traceback.print_exc()
            raise

    def _resolve_comprehensive_market(self, bet: Dict, r_result: Dict, line_value: Optional[float],
                                      direction: Optional[str] = None) -> Tuple[bool, float]:
        """Comprehensive market resolution that handles ALL bet types"""
        player_name = bet.get("player", "")
        market = bet.get("market", "").lower()
        stake = float(bet.get("stake", 0))
        odds = float(bet.get("odds", 0))

        # Get data from R result
        data = r_result.get("data", {})
        stats = data.get("stats", {})

        # Check if this is a scorer market
        if "first" in market and ("scorer" in market or "td" in market):
            # First scorer market
            first_scorer = data.get("first_scorer", "")
            won = self._player_name_matches(player_name, first_scorer)
            print(f"[DEBUG] First scorer check: {player_name} vs {first_scorer} -> {won}")

        elif "last" in market and ("scorer" in market or "td" in market):
            # Last scorer market
            last_scorer = data.get("last_scorer", "")
            won = self._player_name_matches(player_name, last_scorer)
            print(f"[DEBUG] Last scorer check: {player_name} vs {last_scorer} -> {won}")

        elif "scorer" in market or "td" in market or "score" in market:
            # Anytime scorer market
            total_tds = stats.get("touchdowns", 0) or stats.get("total_tds", 0)
            won = total_tds > 0
            print(f"[DEBUG] Anytime scorer check: {player_name} had {total_tds} TDs -> {won}")

        elif "receptions" in market:
            # Receptions market
            actual_value = stats.get("receptions", 0)
            won = self._compare_with_direction(actual_value, line_value, direction, market)
            print(f"[DEBUG] Receptions: {actual_value} vs line {line_value} ({direction}) -> {won}")

        elif "receiving yards" in market:
            # Receiving yards market
            actual_value = stats.get("receiving_yards", 0)
            won = self._compare_with_direction(actual_value, line_value, direction, market)
            print(f"[DEBUG] Receiving yards: {actual_value} vs line {line_value} ({direction}) -> {won}")

        elif "rushing yards" in market:
            # Rushing yards market
            actual_value = stats.get("rushing_yards", 0)
            won = self._compare_with_direction(actual_value, line_value, direction, market)
            print(f"[DEBUG] Rushing yards: {actual_value} vs line {line_value} ({direction}) -> {won}")

        elif "passing yards" in market:
            # Passing yards market
            actual_value = stats.get("passing_yards", 0)
            won = self._compare_with_direction(actual_value, line_value, direction, market)
            print(f"[DEBUG] Passing yards: {actual_value} vs line {line_value} ({direction}) -> {won}")

        elif "yards" in market:
            # Generic yards market
            passing = stats.get("passing_yards", 0)
            rushing = stats.get("rushing_yards", 0)
            receiving = stats.get("receiving_yards", 0)
            actual_value = passing + rushing + receiving
            won = self._compare_with_direction(actual_value, line_value, direction, market)
            print(
                f"[DEBUG] Total yards: {actual_value} (P:{passing}, R:{rushing}, REC:{receiving}) vs line {line_value} ({direction}) -> {won}")

        elif "touchdowns" in market or "td" in market:
            # Touchdowns market
            actual_value = stats.get("touchdowns", 0) or stats.get("total_tds", 0)
            won = self._compare_with_direction(actual_value, line_value, direction, market)
            print(f"[DEBUG] Touchdowns: {actual_value} vs line {line_value} ({direction}) -> {won}")

        elif "points" in market:
            # Points market (basketball)
            actual_value = stats.get("points", 0)
            won = self._compare_with_direction(actual_value, line_value, direction, market)
            print(f"[DEBUG] Points: {actual_value} vs line {line_value} ({direction}) -> {won}")

        else:
            # Fallback: try to determine stat from market
            print(f"[WARNING] Unknown market type: {market}. Using fallback logic.")
            actual_value = self._extract_stat_from_market(market, stats)
            won = self._compare_with_direction(actual_value, line_value, direction, market)
            print(f"[DEBUG] Fallback: {actual_value} vs line {line_value} ({direction}) -> {won}")

        # Calculate profit
        profit = stake * (odds - 1) if won else -stake

        return won, profit

    def _compare_with_direction(self, actual_value: float, line_value: Optional[float],
                                direction: Optional[str], market: str) -> bool:
        """Compare actual value with line value based on direction"""
        # Determine direction from market if not provided
        if not direction:
            if "over" in market:
                direction = "over"
            elif "under" in market:
                direction = "under"
            else:
                direction = "over"  # Default

        if line_value is None:
            # Boolean market (e.g., "player to score a touchdown")
            return actual_value > 0
        elif direction == "over":
            return actual_value > line_value
        elif direction == "under":
            return actual_value < line_value
        else:
            # Default to over
            return actual_value > line_value

    def _extract_stat_from_market(self, market: str, stats: Dict) -> float:
        """Extract relevant stat from market name"""
        if "yards" in market:
            if "receiving" in market:
                return stats.get("receiving_yards", 0)
            elif "rushing" in market:
                return stats.get("rushing_yards", 0)
            elif "passing" in market:
                return stats.get("passing_yards", 0)
            else:
                # Total yards
                return (stats.get("passing_yards", 0) +
                        stats.get("rushing_yards", 0) +
                        stats.get("receiving_yards", 0))
        elif "receptions" in market:
            return stats.get("receptions", 0)
        elif "touchdown" in market or "td" in market:
            return stats.get("touchdowns", 0) or stats.get("total_tds", 0)
        elif "points" in market:
            return stats.get("points", 0)
        else:
            # Default to first available stat
            for stat_value in stats.values():
                if isinstance(stat_value, (int, float)):
                    return stat_value
            return 0

    def _player_name_matches(self, name1: str, name2: str) -> bool:
        """Check if two player names match (fuzzy)"""
        if not name1 or not name2:
            return False

        # Simple matching
        clean1 = name1.lower().replace('.', '').replace('jr', '').replace('sr', '').strip()
        clean2 = name2.lower().replace('.', '').replace('jr', '').replace('sr', '').strip()

        return (clean1 == clean2 or
                clean1 in clean2 or
                clean2 in clean1 or
                clean1.split()[-1] == clean2.split()[-1])

    def _parse_r_output(self, stdout: str, stderr: str) -> Dict:
        """
        Parse R output intelligently, ignoring package loading messages
        """
        # Combine outputs
        all_output = stdout + stderr

        # Check if this is just package loading (German or English)
        package_loading_keywords = [
            "Lade nötiges Paket", "Lade Paket", "Loading required package",
            "nflreadr", "hoopR", "dplyr", "jsonlite", "stringr", "lubridate"
        ]

        # If it's mostly package loading messages, look harder for JSON
        is_package_loading = any(keyword in all_output for keyword in package_loading_keywords)

        # Try to find JSON - look for the last line that looks like JSON
        lines = all_output.strip().split('\n')
        json_line = None

        # Search from the end (JSON is usually last)
        for line in reversed(lines):
            line = line.strip()
            if line.startswith('{') and line.endswith('}'):
                json_line = line
                break

        if not json_line and is_package_loading:
            # Try more aggressively if it's just package messages
            import re
            json_match = re.search(r'(\{.*\})', all_output, re.DOTALL)
            if json_match:
                json_line = json_match.group(1)

        if not json_line:
            return {
                "success": False,
                "error": f"No JSON found in R output. Package loading only: {is_package_loading}"
            }

        try:
            return json.loads(json_line)
        except json.JSONDecodeError as e:
            return {
                "success": False,
                "error": f"JSON decode error: {e}. JSON line: {json_line[:100]}..."
            }

    # ------------------------------------------------------------------
    # 3. R INVOCATION WITH PRODUCTION SAFEGUARDS
    # ------------------------------------------------------------------
    def _call_r_script(
            self,
            player_name: str,
            sport: str,
            season: int,
            market_type: str,
            event_string: str,
            line_value: Optional[float],
            game_date: Optional[str] = None
    ) -> Dict:
        """Call R script with production safeguards - 7 PARAMETERS ONLY"""

        # Create cache key (collision-safe)
        cache_parts = {
            "player": player_name.lower().strip(),
            "sport": sport,
            "season": season,
            "market": market_type.lower().strip(),
            "event": event_string.lower().strip(),
            "line": line_value,
            "game_date": game_date
        }
        cache_key = hashlib.sha256(
            json.dumps(cache_parts, sort_keys=True).encode()
        ).hexdigest()

        # Check cache
        cached = self._get_cached_result(cache_key)
        if cached:
            self.logger.debug(f"Cache hit for {player_name}")
            return cached

        self.r_calls += 1

        try:
            # Build command with exactly 7 arguments for R script
            cmd = [
                self.rscript_path,
                self.r_script_path,
                player_name,  # Argument 1 - R expects "player_name"
                sport,  # Argument 2 - R expects "sport"
                str(season),  # Argument 3 - R expects "season"
                market_type,  # Argument 4 - R expects "market_type"
                event_string,  # Argument 5 - R expects "event_string"
            ]

            # Handle line value (argument 6)
            if line_value is not None:
                cmd.append(str(line_value))
            else:
                cmd.append("NULL")  # R script expects "NULL" for missing values

            # Handle game_date (argument 7) - ALWAYS pass it
            if game_date:
                cmd.append(game_date)
            else:
                cmd.append("NULL")  # Pass "NULL" if no game_date provided

            self.logger.debug(f"R command: {' '.join(cmd[:3])}...")
            self.logger.debug(f"Full command: {' '.join(cmd)}")

            # Run with timeout and proper error handling
            env = os.environ.copy()
            creation_flags = subprocess.CREATE_NO_WINDOW if self.is_windows else 0

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=90,  # Generous timeout for R data loading
                encoding="utf-8",
                errors="replace",
                env=env,
                creationflags=creation_flags
            )

            # ========== EXTENSIVE DEBUGGING ==========
            self.logger.debug(f"R script return code: {result.returncode}")
            self.logger.debug(f"R script stdout length: {len(result.stdout)}")
            self.logger.debug(f"R script stderr length: {len(result.stderr)}")

            # Log first 500 chars of output for debugging
            if result.stdout:
                self.logger.debug(f"R stdout (first 500 chars): {result.stdout[:500]}")
            if result.stderr:
                self.logger.debug(f"R stderr (first 500 chars): {result.stderr[:500]}")

            # Log ALL output if short enough
            if len(result.stdout + result.stderr) < 2000:
                self.logger.debug(f"ALL R OUTPUT:\n{result.stdout + result.stderr}")

            # ========== Check for package loading messages ==========
            package_loading_indicators = [
                "Lade nötiges Paket",  # German: "Loading required package"
                "Lade Paket",  # German: "Loading package"
                "Loading required package",  # English
                "Loading package",  # English
                "Warnmeldung",  # German: "Warning message"
                "Warning message",  # English
                "Attache Paket",  # German: "Attach package"
                "Attaching package",  # English
                "Das folgende Objekt",  # German: "The following object"
                "The following object",  # English
                "Wird geladen",  # German: "Is being loaded"
                "package",  # Generic
            ]

            is_just_package_loading = False
            has_package_message = False
            has_real_error = False

            if result.stderr:
                stderr_lower = result.stderr.lower()

                # Check for package messages
                for indicator in package_loading_indicators:
                    if indicator.lower() in stderr_lower:
                        has_package_message = True
                        break

                # Check for real errors
                error_keywords = ['error:', 'fehler:', 'failed', 'ungültig', 'invalid',
                                  'cannot', 'could not', 'not found', 'no such', 'missing']
                for keyword in error_keywords:
                    if keyword in stderr_lower:
                        has_real_error = True
                        break

            # If it has package messages but no real errors, it's probably OK
            if has_package_message and not has_real_error:
                is_just_package_loading = True
                self.logger.debug("R stderr appears to be just package loading messages")

            # ========== JSON extraction ==========
            json_line = None

            # Strategy 1: Look for JSON in stdout first (most common)
            if result.stdout:
                for line in result.stdout.strip().split("\n"):
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        json_line = line
                        self.logger.debug("Found JSON in stdout")
                        break

            # Strategy 2: Look in stderr if not found in stdout
            if not json_line and result.stderr:
                for line in result.stderr.strip().split("\n"):
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        json_line = line
                        self.logger.debug("Found JSON in stderr")
                        break

            # Strategy 3: Try to extract JSON from combined output
            if not json_line:
                all_output = result.stdout + result.stderr
                import re
                # Look for JSON pattern
                json_match = re.search(r'(\{.*?\})', all_output, re.DOTALL)
                if json_match:
                    json_line = json_match.group(1)
                    self.logger.debug("Extracted JSON from combined output")

            # If no JSON found at all
            if not json_line:
                error_msg = f"No JSON from R. Return code: {result.returncode}. "
                error_msg += f"Stdout length: {len(result.stdout)}, Stderr length: {len(result.stderr)}"

                # Log all output for debugging
                self.logger.error(f"NO JSON FOUND. Full output:\n{result.stdout}\n{result.stderr}")

                # Special handling for college games
                all_output = result.stdout + result.stderr
                if any(college_word in all_output.lower() for college_word in
                       ['college', 'ncaa', 'oregon', 'texas tech']):
                    return {
                        "success": False,
                        "error": "College (NCAA) game detected. Only NFL/NBA games are supported.",
                        "college_game": True
                    }

                # If it's just package loading and return code is 0, the script might have run
                if is_just_package_loading and result.returncode == 0:
                    self.logger.warning("R script may have run but produced no JSON output")
                    return {"success": False, "error": "R script ran but produced no JSON output"}
                else:
                    self.logger.error(f"{error_msg}")
                    return {"success": False, "error": "No valid JSON from R"}

            # Try to parse the JSON
            try:
                data = json.loads(json_line)
                self.r_successful += 1

                self.logger.debug(
                    f"Successfully parsed JSON from R: success={data.get('success')}, resolved={data.get('resolved')}")

                # Check for college game error from R
                if not data.get("success") and any(college_word in data.get("error", "").lower()
                                                   for college_word in ['college', 'ncaa', 'oregon']):
                    data[
                        "error"] = f"College game detected: {data.get('error', 'NCAA game')}. Only NFL/NBA supported."

                # Cache successful results
                if data.get("success") and data.get("resolved"):
                    # Use cache_key directly since we removed bet_key parameter
                    self._set_cached_result(cache_key, data, ttl_hours=24)

                return data

            except json.JSONDecodeError as e:
                self.logger.error(f"JSON decode error: {e}")
                self.logger.error(f"JSON line that failed: {json_line[:200]}...")

                # Check if output contains college indicators
                all_output = result.stdout + result.stderr
                if any(college_word in all_output.lower() for college_word in
                       ['college', 'ncaa', 'oregon', 'texas tech']):
                    return {
                        "success": False,
                        "error": "College (NCAA) game detected. Only NFL/NBA games are supported.",
                        "college_game": True
                    }

                return {"success": False, "error": f"Invalid JSON from R: {e}"}

        except subprocess.TimeoutExpired:
            self.logger.error("R script timeout (90s)")
            return {"success": False, "error": "R script timeout"}

        except Exception as e:
            self.logger.error(f"Error calling R script: {e}")
            return {"success": False, "error": f"Python error: {str(e)}"}

    def _debug_r_communication(self, cmd, result):
        """Debug R communication issues"""
        self.logger.debug("=" * 80)
        self.logger.debug("R COMMUNICATION DEBUG - DETAILED")
        self.logger.debug("=" * 80)
        self.logger.debug(f"Command: {' '.join(cmd)}")
        self.logger.debug(f"Return code: {result.returncode}")
        self.logger.debug(f"Stdout length: {len(result.stdout)} chars")
        self.logger.debug(f"Stderr length: {len(result.stderr)} chars")

        if result.stdout:
            self.logger.debug("Stdout (first 500 chars):")
            self.logger.debug(result.stdout[:500])

        if result.stderr:
            self.logger.debug("Stderr (first 500 chars):")
            self.logger.debug(result.stderr[:500])

        # Look for JSON in output
        all_output = result.stdout + result.stderr
        lines = all_output.split('\n')
        for i, line in enumerate(lines):
            if line.strip().startswith('{'):
                self.logger.debug(f"Found possible JSON on line {i}: {line[:200]}...")

        self.logger.debug("=" * 80)

    # ------------------------------------------------------------------
    # 4. HELPER METHODS (Critical fixes included)
    # ------------------------------------------------------------------

    def _extract_season_hint(self, event: str, sport: str, game_date: Optional[str] = None) -> int:
        """
        Extract season hint for R data loading
        Uses game_date if available, otherwise falls back to event parsing
        """
        # ========== PRIORITY 1: Use game_date from bet ==========
        if game_date:
            try:
                game_date_obj = datetime.strptime(game_date, "%Y-%m-%d").date()

                if sport == "nfl":
                    # NFL season: Games in Jan-Feb belong to previous year
                    if game_date_obj.month <= 2:
                        return game_date_obj.year - 1
                    else:
                        return game_date_obj.year

                elif sport == "nba" or sport == "basketball":
                    # NBA season: Games Oct-June, spanning years
                    # 2024-25 season: games from Oct 2024 to June 2025
                    if game_date_obj.month >= 10:
                        return game_date_obj.year  # 2024-25 season starts in 2024
                    else:
                        return game_date_obj.year - 1  # 2023-24 season ends in 2024

                # Default for other sports
                return game_date_obj.year

            except Exception as e:
                self.logger.debug(f"Could not parse game_date '{game_date}' for season: {e}")
                # Fall through to event parsing

        # ========== PRIORITY 2: Try to extract date from event ==========
        try:
            date_match = re.search(r"(\d{4}-\d{2}-\d{2})", event)
            if date_match:
                game_date_str = date_match.group(1)
                game_date_obj = datetime.strptime(game_date_str, "%Y-%m-%d").date()

                if sport == "nfl":
                    if game_date_obj.month <= 2:
                        return game_date_obj.year - 1
                    else:
                        return game_date_obj.year

                elif sport == "nba" or sport == "basketball":
                    if game_date_obj.month >= 10:
                        return game_date_obj.year
                    else:
                        return game_date_obj.year - 1

                return game_date_obj.year
        except Exception as e:
            self.logger.debug(f"Could not extract date from event for season: {e}")

        # ========== FALLBACK: Use current time ==========
        current_date = datetime.now().date()

        if sport == "nfl":
            if current_date.month <= 2:
                return current_date.year - 1
            else:
                return current_date.year

        elif sport == "nba" or sport == "basketball":
            if current_date.month >= 10:
                return current_date.year
            else:
                return current_date.year - 1

        return current_date.year

    def _get_bet_key(self, bet: Dict) -> str:
        """Create unique, deterministic bet key"""
        components = {
            "player": (bet.get("player") or bet.get("player_name", "")).lower().strip(),
            "event": bet.get("event", "").lower().strip(),
            "market": bet.get("market", "").lower().strip(),
            "odds": str(bet.get("odds", "")),
            "stake": str(bet.get("stake", ""))
        }
        return hashlib.md5(
            json.dumps(components, sort_keys=True).encode()
        ).hexdigest()

    def _is_retryable_error(self, error_msg: str) -> bool:
        """Determine if an error is retryable"""
        error_lower = error_msg.lower()
        retryable_phrases = [
            "not yet ready",
            "not resolved",
            "game not complete",
            "not available",
            "data not available",
            "schedule not found",
            "timeout",
            "temporarily unavailable"
        ]

        return any(phrase in error_lower for phrase in retryable_phrases)

    # ------------------------------------------------------------------
    # 5. CACHE SYSTEM (Production-grade)
    # ------------------------------------------------------------------

    def _init_cache(self) -> Optional[sqlite3.Connection]:
        """Initialize SQLite cache"""
        try:
            db_path = self.config.get("cache_db", "r_stats_cache.db")
            conn = sqlite3.connect(db_path, check_same_thread=False)
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cache (
                    cache_key TEXT PRIMARY KEY,
                    data_json TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP,
                    bet_key TEXT,
                    player TEXT,
                    sport TEXT
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_expires ON cache(expires_at)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bet_key ON cache(bet_key)")
            conn.commit()
            self.logger.info("Cache database initialized")
            return conn
        except Exception as e:
            self.logger.warning(f"Could not initialize cache: {e}")
            return None

    def _get_cached_result(self, cache_key: str) -> Optional[Dict]:
        """Get cached result with expiry check"""
        if not self.cache_db:
            return None

        try:
            cursor = self.cache_db.cursor()
            cursor.execute("""
                SELECT data_json FROM cache 
                WHERE cache_key = ? AND (expires_at IS NULL OR expires_at > ?)
            """, (cache_key, datetime.now().isoformat()))

            row = cursor.fetchone()
            if row:
                return json.loads(row[0])
        except Exception as e:
            self.logger.debug(f"Cache read error: {e}")

        return None

    def _set_cached_result(self, cache_key: str, data: Dict, ttl_hours: int = 24, bet_key: str = None):
        """Cache result with TTL"""
        if not self.cache_db:
            return

        try:
            expires_at = (datetime.now() + timedelta(hours=ttl_hours)).isoformat()
            cursor = self.cache_db.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO cache 
                (cache_key, data_json, expires_at, bet_key, player, sport)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                cache_key,
                json.dumps(data),
                expires_at,
                bet_key,
                data.get("player"),
                data.get("sport")
            ))
            self.cache_db.commit()
        except Exception as e:
            self.logger.debug(f"Cache write error: {e}")

    # ------------------------------------------------------------------
    # 6. RESOLUTION LOGIC
    # ------------------------------------------------------------------

    def _determine_special_market_type(self, market: str) -> str:
        market_lower = market.lower()
        if "first" in market_lower and ("scorer" in market_lower or "td" in market_lower):
            return "first_scorer"
        elif "last" in market_lower and ("scorer" in market_lower or "td" in market_lower):
            return "last_scorer"
        elif "scorer" in market_lower:
            return "first_scorer"
        else:
            return "player_prop"

    def _resolve_special_market(self, bet: Dict, r_result: Dict) -> Tuple[bool, float]:
        player_name = bet.get("player", "")
        stake = float(bet.get("stake", 0))
        odds = float(bet.get("odds", 0))

        if r_result.get("player_scored", False):
            won = True
            profit = stake * (odds - 1)
            self.logger.info(f"{player_name} was the scorer!")
        else:
            won = False
            profit = -stake
            self.logger.info(f"{player_name} was not the scorer")

        return won, profit

    # ------------------------------------------------------------------
    # 7. UTILITIES AND MONITORING
    # ------------------------------------------------------------------

    def _find_rscript(self) -> Optional[str]:
        """Find Rscript executable"""
        possible_paths = [
            r"C:\Program Files\R\R-4.5.1\bin\x64\Rscript.exe",
            r"C:\Program Files\R\R-4.5.1\bin\Rscript.exe",
            "/usr/local/bin/Rscript",
            "/usr/bin/Rscript",
            "Rscript"  # Try PATH
        ]

        for path in possible_paths:
            if path == "Rscript":
                try:
                    result = subprocess.run([path, "--version"],
                                            capture_output=True, text=True, timeout=5)
                    if result.returncode == 0:
                        return path
                except:
                    continue
            elif os.path.exists(path):
                try:
                    result = subprocess.run([path, "-e", "cat('OK')"],
                                            capture_output=True, text=True, timeout=5)
                    if result.returncode == 0:
                        return path
                except:
                    continue

        return None

    def get_resolution_state(self, bet: Dict) -> Dict:
        """Get detailed resolution state for a bet"""
        bet_key = self._get_bet_key(bet)
        attempt_info = self.resolution_attempts.get(bet_key)

        if not attempt_info:
            return {"status": "never_attempted", "bet_key": bet_key}

        return {
            "status": "pending_retry" if attempt_info['attempts'] > 0 else "ready",
            "attempts": attempt_info['attempts'],
            "last_attempt": attempt_info['last_attempt'].isoformat() if attempt_info['last_attempt'] else None,
            "last_error": attempt_info['last_error'],
            "max_attempts": self.max_attempts_per_bet,
            "can_retry": attempt_info['attempts'] < self.max_attempts_per_bet,
            "next_retry_hours": self.retry_cooldown_hours,
            "bet_key": bet_key
        }

    def get_performance_stats(self) -> Dict:
        """Get comprehensive performance statistics"""
        active_attempts = len([a for a in self.resolution_attempts.values()
                               if a['attempts'] > 0])

        total_attempts = sum(a['attempts'] for a in self.resolution_attempts.values())

        return {
            "r_calls_total": self.r_calls,
            "r_calls_successful": self.r_successful,
            "r_success_rate": (self.r_successful / self.r_calls * 100) if self.r_calls > 0 else 0,
            "active_bets": len(self.resolution_attempts),
            "active_resolution_attempts": active_attempts,
            "total_resolution_attempts": total_attempts,
            "cache_size": self._get_cache_size(),
            "retry_cooldown_hours": self.retry_cooldown_hours,
            "max_attempts_per_bet": self.max_attempts_per_bet
        }

    def _get_cache_size(self) -> int:
        """Get cache size"""
        if not self.cache_db:
            return 0

        try:
            cursor = self.cache_db.cursor()
            cursor.execute("SELECT COUNT(*) FROM cache")
            return cursor.fetchone()[0]
        except:
            return 0

    def cleanup_old_attempts(self, max_age_hours: int = 168):
        """Clean up old resolution attempts (1 week default)"""
        cutoff = datetime.now() - timedelta(hours=max_age_hours)
        to_remove = []

        for bet_key, info in self.resolution_attempts.items():
            last_attempt = info.get('last_attempt')
            if last_attempt and last_attempt < cutoff:
                to_remove.append(bet_key)

        for bet_key in to_remove:
            self.resolution_attempts.pop(bet_key, None)

        self.logger.info(f"Cleaned up {len(to_remove)} old resolution attempts")

    def reset_bet_attempts(self, bet: Dict = None, bet_key: str = None):
        """Reset resolution attempts for a specific bet"""
        if bet:
            bet_key = self._get_bet_key(bet)

        if bet_key and bet_key in self.resolution_attempts:
            self.resolution_attempts.pop(bet_key)
            self.logger.info(f"Reset resolution attempts for bet {bet_key}")

    def _is_college_game(self, event: str, sport: str) -> bool:
        """Check if this is a college game that shouldn't be resolved"""
        event_lower = event.lower()
        sport_lower = sport.lower()

        # College football indicators
        college_keywords = [
            'oregon', 'texas tech', 'ncaa', 'college',
            'clemson', 'alabama', 'georgia', 'michigan', 'ohio state',
            'usc', 'stanford', 'florida state', 'lsu', 'oklahoma',
            'notre dame', 'pen state', 'tennessee', 'utah'
        ]

        # Check if this looks like college football
        if sport_lower in ['football', 'nfl']:
            for keyword in college_keywords:
                if keyword in event_lower:
                    return True

        # Check for NCAA/FBS/FCS indicators
        if any(college_marker in event_lower for college_marker in ['ncaa', 'fbs', 'fcs', 'college']):
            return True

        return False
# =============================================================================
# IDEMPOTENT RESOLUTION MANAGER
# =============================================================================

class IdempotentResolutionManager:
    """
    Production-grade state machine for bet resolution
    Guarantees: no double resolutions, safe retries, audit trail
    """

    def __init__(self, db_manager, r_resolver: RStatsResolver):
        self.db = db_manager
        self.resolver = r_resolver

        # State definitions
        self.STATES = {
            'PENDING': 'pending',
            'RESOLVING': 'resolving',
            'COMPLETE': 'complete',
            'FAILED_PERMANENT': 'failed_permanent',
            'RETRY_PENDING': 'retry_pending'
        }

        # Audit trail
        self.audit_log = []

    def _is_future_game(self, game_date: str) -> bool:
        """Check if a game date is in the future - FIXED VERSION"""
        if not game_date:
            return False

        try:
            # Parse the game date
            game_date_obj = datetime.strptime(str(game_date), '%Y-%m-%d').date()
            current_date = datetime.now().date()

            print(f"[DEBUG DATE CHECK] Game: {game_date_obj}, Current: {current_date}")

            # If game is in the future (more than 1 day ahead)
            if game_date_obj > current_date:
                print(f"[DEBUG] Game {game_date} IS in the future")
                return True

            # If game is today or yesterday, we might need to wait
            if game_date_obj == current_date:
                current_hour = datetime.now().hour
                # Most NFL/NBA games are in the evening, so wait until after 9 PM
                if current_hour < 21:  # 9 PM
                    print(f"[DEBUG] Game {game_date} is today, but before 9 PM")
                    return True

            # Game is in the past - should be resolvable!
            print(f"[DEBUG] Game {game_date} is in the PAST - should be resolvable")
            return False

        except Exception as e:
            print(f"Could not parse game date '{game_date}': {e}")
            return False
    def resolve_bet_safe(self, bet_id: str, force: bool = False) -> Dict:
        """
        Idempotent resolution with full state tracking
        Returns: {
            'state': current state,
            'result': win/loss if complete,
            'profit': amount if complete,
            'error': error message if failed,
            'resolution_id': unique ID for this attempt,
            'audit_trail': list of state changes
        }
        """
        resolution_id = f"res_{bet_id}_{int(time.time())}"

        print(f"\n[DEBUG resolve_bet_safe] Starting resolution for bet {bet_id}")

        try:
            # 1. Get bet and current state
            bet = self.db.get_bet(bet_id)
            if not bet:
                print(f"  [ERROR] Bet {bet_id} not found in database")
                return self._create_result('failed_permanent',
                                           error=f"Bet {bet_id} not found",
                                           resolution_id=resolution_id)

            # ========== ADDED: CHECK FOR FUTURE GAMES ==========
            game_date = bet.get('game_date')
            if game_date and self._is_future_game(game_date):
                days_until = (datetime.strptime(str(game_date), '%Y-%m-%d').date() - datetime.now().date()).days
                print(f"  [SKIP] Game is in the future: {game_date} ({days_until} days from now)")
                print(f"  [SKIP] Player: {bet.get('player', 'Unknown')} - {bet.get('market', 'Unknown')}")
                print(f"  [SKIP] Event: {bet.get('event', 'Unknown')}")

                return self._create_result(
                    'retry_pending',
                    error=f"Game is in the future ({game_date}, {days_until} days)",
                    next_retry=(datetime.now() + timedelta(days=1)).isoformat(),  # Try again tomorrow
                    resolution_id=resolution_id
                )
            # ========== END ADDED ==========

            # ========== FIXED: Use CORRECT DATABASE COLUMN NAMES ==========
            print(f"  [DEBUG] Got bet data (ACTUAL DATABASE COLUMNS):")
            print(f"    Player (from 'player' column): {bet.get('player')}")
            print(f"    Sport: {bet.get('sport')}")
            print(f"    Market (from 'market' column): {bet.get('market')}")
            print(f"    Event (from 'event' column): {bet.get('event')}")
            print(f"    Game Date: {bet.get('game_date')}")
            print(f"    Stake: {bet.get('stake')}")
            print(f"    Odds: {bet.get('odds')}")
            print(f"    Status: {bet.get('status')}")

            # Show all fields for debugging
            print(f"  [DEBUG] All bet fields:")
            for key, value in bet.items():
                if value:  # Only show non-empty values
                    print(f"    {key}: {value}")

            current_state = bet.get('resolution_state', self.STATES['PENDING'])
            print(f"  [DEBUG] Current resolution state: {current_state}")

            # 2. Check state transitions
            if current_state == self.STATES['COMPLETE']:
                # Already resolved - return existing result
                print(f"  [DEBUG] Bet already complete, returning existing result")
                return self._create_result(
                    'complete',
                    result=bet.get('result'),
                    profit=bet.get('profit'),
                    resolution_id=resolution_id
                )

            elif current_state == self.STATES['FAILED_PERMANENT']:
                # Permanent failure - no retry
                print(f"  [DEBUG] Bet has permanent failure, no retry")
                return self._create_result(
                    'failed_permanent',
                    error=bet.get('resolution_error', 'Previous permanent failure'),
                    resolution_id=resolution_id
                )

            elif current_state == self.STATES['RETRY_PENDING']:
                # Check if enough time has passed
                last_attempt = bet.get('last_resolution_attempt')
                if last_attempt:
                    last_dt = datetime.fromisoformat(last_attempt.replace('Z', '+00:00'))
                    hours_since = (datetime.now() - last_dt).total_seconds() / 3600

                    if hours_since < self.resolver.retry_cooldown_hours and not force:
                        next_retry = last_dt + timedelta(hours=self.resolver.retry_cooldown_hours)
                        print(f"  [DEBUG] Retry cooldown active: {hours_since:.1f}h since last attempt")
                        return self._create_result(
                            'retry_pending',
                            error=f"Retry cooldown active: {hours_since:.1f}h since last attempt",
                            next_retry=next_retry.isoformat(),
                            resolution_id=resolution_id
                        )

            # 3. Attempt resolution
            print(f"  [DEBUG] Attempting resolution...")
            self._update_bet_state(bet_id, self.STATES['RESOLVING'],
                                   f"Starting resolution {resolution_id}")

            # Add force flag if requested
            resolution_bet = bet.copy()
            if force:
                resolution_bet['force_resolve'] = True
                print(f"  [DEBUG] Force resolution enabled")

            try:
                # Call resolver
                print(f"  [DEBUG] Calling resolver.resolve_player_prop()...")
                won, profit = self.resolver.resolve_player_prop(resolution_bet)

                print(f"  [DEBUG] Resolver returned: won={won}, profit={profit}")

                # Update database
                self.db.update_bet_result(bet_id, won, profit)
                self._update_bet_state(bet_id, self.STATES['COMPLETE'],
                                       f"Successfully resolved: {'WIN' if won else 'LOSS'}")

                # Record success
                self._audit('success', bet_id, resolution_id,
                            f"Resolved: {won}, Profit: {profit}")

                return self._create_result(
                    'complete',
                    result='win' if won else 'loss',
                    profit=profit,
                    resolution_id=resolution_id
                )

            except Exception as e:
                error_msg = str(e)
                print(f"  [ERROR] Resolver exception: {error_msg}")
                import traceback
                traceback.print_exc()

                # Determine error type
                if self.resolver._is_retryable_error(error_msg):
                    # Schedule retry
                    next_retry = datetime.now() + timedelta(hours=self.resolver.retry_cooldown_hours)

                    self._update_bet_state(bet_id, self.STATES['RETRY_PENDING'],
                                           f"Retryable error: {error_msg}")
                    self.db.update_bet_error(bet_id, error_msg)

                    self._audit('retry_scheduled', bet_id, resolution_id,
                                f"Error: {error_msg}, Next retry: {next_retry.isoformat()}")

                    return self._create_result(
                        'retry_pending',
                        error=error_msg,
                        next_retry=next_retry.isoformat(),
                        resolution_id=resolution_id
                    )

                else:
                    # Permanent failure
                    self._update_bet_state(bet_id, self.STATES['FAILED_PERMANENT'],
                                           f"Permanent failure: {error_msg}")
                    self.db.update_bet_error(bet_id, error_msg)

                    self._audit('permanent_failure', bet_id, resolution_id,
                                f"Permanent error: {error_msg}")

                    return self._create_result(
                        'failed_permanent',
                        error=error_msg,
                        resolution_id=resolution_id
                    )

        except Exception as e:
            # Manager-level error
            error_msg = f"Resolution manager error: {str(e)}"
            print(f"  [ERROR] Manager exception: {error_msg}")
            import traceback
            traceback.print_exc()
            self._audit('manager_error', bet_id, resolution_id, error_msg)

            return self._create_result(
                'failed_permanent',
                error=error_msg,
                resolution_id=resolution_id
            )

    def batch_resolve(self, max_bets: int = 10, force: bool = False) -> Dict:
        """Batch resolution with rate limiting"""
        pending_bets = self.db.get_pending_bets(limit=max_bets)
        results = {
            'total': len(pending_bets),
            'complete': 0,
            'failed_permanent': 0,
            'retry_pending': 0,
            'future_games': 0,  # ADDED: Track future games
            'manager_errors': 0,
            'batch_id': f"batch_{int(time.time())}",
            'details': []
        }

        for bet in pending_bets:
            # ========== ADDED: Check for future game before attempting resolution ==========
            game_date = bet.get('game_date')
            if game_date and self._is_future_game(game_date):
                days_until = (datetime.strptime(str(game_date), '%Y-%m-%d').date() - datetime.now().date()).days
                print(
                    f"⏭️  Skipping future game ({days_until} days away): {bet.get('player', 'Unknown')} - {game_date}")
                results['future_games'] += 1
                results['details'].append({
                    'bet_id': bet['id'],
                    'player': bet.get('player'),
                    'event': bet.get('event'),
                    'result': {
                        'state': 'future_game',
                        'error': f"Game is in the future ({game_date})",
                        'game_date': game_date,
                        'days_until': days_until
                    }
                })
                continue  # Skip to next bet
            # ========== END ADDED ==========

            result = self.resolve_bet_safe(bet['id'], force)
            results['details'].append({
                'bet_id': bet['id'],
                'player': bet.get('player'),
                'event': bet.get('event'),
                'result': result
            })

            if result['state'] == 'complete':
                results['complete'] += 1
            elif result['state'] == 'failed_permanent':
                results['failed_permanent'] += 1
            elif result['state'] == 'retry_pending':
                results['retry_pending'] += 1
            else:
                results['manager_errors'] += 1

            # Rate limiting
            time.sleep(0.5)

        return results

    # Rest of the class remains the same...

    # Helper methods
    def _update_bet_state(self, bet_id: str, state: str, note: str = ""):
        """Update bet state with audit trail"""
        try:
            if hasattr(self.db, 'update_bet_state'):
                self.db.update_bet_state(bet_id, state, note)
            else:
                # Fallback: update via existing methods
                if state in ['complete', 'retry_pending', 'failed_permanent']:
                    # This would need to be implemented in your database manager
                    # For now, just log it
                    print(f"Would update bet {bet_id} state to {state}: {note}")
        except Exception as e:
            print(f"Error updating bet state: {e}")

    def _create_result(self, state: str, **kwargs) -> Dict:
        """Create standardized result dict"""
        result = {'state': state, **kwargs}
        result['timestamp'] = datetime.now().isoformat()
        return result

    def _audit(self, event_type: str, bet_id: str, resolution_id: str, message: str):
        """Record audit trail"""
        entry = {
            'timestamp': datetime.now().isoformat(),
            'event': event_type,
            'bet_id': bet_id,
            'resolution_id': resolution_id,
            'message': message
        }
        self.audit_log.append(entry)

        # Keep only last 1000 entries
        if len(self.audit_log) > 1000:
            self.audit_log = self.audit_log[-1000:]

    def get_audit_trail(self, bet_id: str = None) -> List[Dict]:
        """Get audit trail for a specific bet or all bets"""
        if bet_id:
            return [entry for entry in self.audit_log if entry['bet_id'] == bet_id]
        return self.audit_log


# =============================================================================
# UNIVERSAL BET PARSER (YOUR VERSION - ENHANCED)
# =============================================================================

class UniversalBetParser:
    """Intelligently parses ANY bet type from CrazyNinjaOdds"""

    def __init__(self):
        self.stat_mappings = {
            'points': {'sources': ['espn', 'basketball_reference'], 'key': 'points'},
            'rebounds': {'sources': ['espn', 'basketball_reference'], 'key': 'rebounds'},
            'assists': {'sources': ['espn', 'basketball_reference'], 'key': 'assists'},
            'touchdowns': {'sources': ['espn', 'pro_football_reference'], 'key': 'touchdowns'},
            'yards': {'sources': ['espn', 'pro_football_reference'], 'key': 'yards'},
            'receptions': {'sources': ['espn', 'pro_football_reference'], 'key': 'receptions'},
            'three_pointers': {'sources': ['espn', 'basketball_reference'], 'key': 'three_pointers'},
        }

    def parse_any_bet(self, sport: str, event: str, market: str, selection: str) -> Dict:
        """
        Universal parser that identifies:
        1. Bet type
        2. Required data to scrape
        3. Resolution logic needed
        """
        result = {
            'bet_type': 'unknown',
            'sport': sport.lower(),
            'event': event,
            'market_raw': market,
            'selection_raw': selection,
            'required_data': [],
            'resolution_logic': 'unknown',
            'parsed_components': {}
        }

        # Identify bet type
        bet_type = self._identify_bet_type(market, selection)
        result['bet_type'] = bet_type

        # Parse components
        components = self._parse_components(bet_type, market, selection, event)
        result['parsed_components'] = components

        # Determine required data
        required_data = self._determine_required_data(bet_type, components, sport)
        result['required_data'] = required_data

        # Set resolution logic
        resolution_logic = self._determine_resolution_logic(bet_type, components)
        result['resolution_logic'] = resolution_logic

        return result

    def _identify_bet_type(self, market: str, selection: str) -> str:
        """Identify what type of bet this is"""
        market_lower = market.lower()
        selection_lower = selection.lower()

        # Player props
        if 'player' in market_lower:
            if 'first' in market_lower and 'scorer' in market_lower:
                return 'player_first_scorer'
            elif 'last' in market_lower and 'scorer' in market_lower:
                return 'player_last_scorer'
            elif 'touchdown' in market_lower or 'td' in market_lower:
                return 'player_touchdown'
            elif any(stat in market_lower for stat in ['points', 'rebounds', 'assists', 'yards', 'receptions']):
                if 'over' in selection_lower or 'under' in selection_lower:
                    return 'player_stat_over_under'
                else:
                    return 'player_stat_exact'
            else:
                return 'player_prop_generic'

        # Team/Game props
        elif 'point spread' in market_lower or 'spread' in market_lower:
            return 'point_spread'
        elif 'moneyline' in market_lower:
            return 'moneyline'
        elif 'total points' in market_lower or 'over/under' in market_lower:
            return 'game_total'
        elif 'quarter' in market_lower and 'total' in market_lower:
            return 'quarter_total'

        # Team props
        elif 'team' in market_lower:
            return 'team_prop'

        return 'unknown'

    def _parse_components(self, bet_type: str, market: str, selection: str, event: str) -> Dict:
        """Extract all relevant components from the bet"""
        components = {}

        # Extract teams from event
        teams = self._extract_teams(event)
        components['teams'] = teams

        if bet_type.startswith('player_'):
            # Player bets
            player_info = self._parse_player_selection(selection)
            components.update(player_info)

            # Extract stat from market
            stat_match = re.search(r'(points|rebounds|assists|yards|touchdowns|receptions|threes)', market.lower())
            if stat_match:
                components['stat'] = stat_match.group(1)

            # Extract line if present
            line_match = re.search(r'(\d+[\.,]?\d*)', selection)
            if line_match:
                components['line'] = float(line_match.group(1).replace(',', '.'))

            # Extract direction
            if 'over' in selection.lower():
                components['direction'] = 'over'
            elif 'under' in selection.lower():
                components['direction'] = 'under'

        elif bet_type in ['point_spread', 'moneyline', 'team_prop']:
            # Team bets
            team_info = self._parse_team_selection(selection, teams)
            components.update(team_info)

        elif bet_type in ['game_total', 'quarter_total']:
            # Total bets
            total_info = self._parse_total_selection(selection)
            components.update(total_info)

        return components

    def _determine_required_data(self, bet_type: str, components: Dict, sport: str) -> List[str]:
        """Determine what data needs to be scraped for this bet"""
        required = []

        if bet_type.startswith('player_'):
            required.append('player_stats')
            if 'first_scorer' in bet_type or 'last_scorer' in bet_type:
                required.append('play_by_play')

        elif bet_type in ['point_spread', 'moneyline', 'game_total', 'team_prop', 'quarter_total']:
            required.append('game_result')

        return required

    def _determine_resolution_logic(self, bet_type: str, components: Dict) -> str:
        """Determine what logic to apply for resolution"""
        if bet_type == 'player_stat_over_under':
            return 'compare_stat_to_line'
        elif bet_type == 'player_first_scorer':
            return 'check_first_scorer'
        elif bet_type == 'player_last_scorer':
            return 'check_last_scorer'
        elif bet_type == 'player_touchdown':
            return 'check_touchdown'
        elif bet_type == 'point_spread':
            return 'calculate_spread'
        elif bet_type == 'moneyline':
            return 'check_winner'
        elif bet_type == 'game_total':
            return 'sum_game_points'
        elif bet_type == 'quarter_total':
            return 'sum_quarter_points'

        return 'generic_comparison'

    # Helper methods
    def _extract_teams(self, event: str) -> List[str]:
        """Extract team names from event string"""
        separators = [' @ ', ' vs ', ' at ', ' - ']
        for sep in separators:
            if sep in event:
                parts = event.split(sep)
                if len(parts) >= 2:
                    return [part.strip() for part in parts[:2]]
        return [event, 'Unknown']

    def _parse_player_selection(self, selection: str) -> Dict:
        """Extract player name from selection"""
        # Remove $ amounts
        clean = re.sub(r'\(\$[^)]+\)', '', selection).strip()

        # Try to find player name (everything before Over/Under)
        patterns = [
            r'^(.*?)\s+over\s+\d+',  # "Player Over X.X"
            r'^(.*?)\s+under\s+\d+',  # "Player Under X.X"
            r'^(.*?)\s+to\s+',  # "Player To Score"
            r'^(.*?)\s+first\s+',  # "Player First"
            r'^(.*?)\s+last\s+',  # "Player Last"
        ]

        for pattern in patterns:
            match = re.match(pattern, clean, re.IGNORECASE)
            if match:
                return {'player': match.group(1).strip().title()}

        # If no pattern matches, assume entire string is player name
        return {'player': clean.title()}

    def _parse_team_selection(self, selection: str, teams: List[str]) -> Dict:
        """Extract team info from selection"""
        # Check if selection matches a team
        for team in teams:
            if team.lower() in selection.lower():
                # Check for spread
                spread_match = re.search(r'([+-]\d+[\.,]?\d*)', selection)
                if spread_match:
                    return {
                        'team': team,
                        'spread': spread_match.group(1),
                        'line': float(spread_match.group(1)[1:].replace(',', '.'))
                    }
                return {'team': team}

        return {'team': selection}

    def _parse_total_selection(self, selection: str) -> Dict:
        """Extract over/under info from totals bet"""
        if 'over' in selection.lower():
            direction = 'over'
        elif 'under' in selection.lower():
            direction = 'under'
        else:
            direction = None

        line_match = re.search(r'(\d+[\.,]?\d*)', selection)
        line = float(line_match.group(1).replace(',', '.')) if line_match else None

        return {'direction': direction, 'line': line}


# =============================================================================
# ENHANCED GAME MATCHER (YOUR VERSION - ENHANCED)
# =============================================================================

class EnhancedGameMatcher:
    def __init__(self):
        self.setup_team_mappings()

    def setup_team_mappings(self):
        """Comprehensive team name mappings"""
        self.team_variations = {
            # NBA teams
            'lakers': ['los angeles lakers', 'la lakers'],
            'warriors': ['golden state warriors', 'gsw'],
            'celtics': ['boston celtics'],
            'bulls': ['chicago bulls'],
            'knicks': ['new york knicks'],
            'heat': ['miami heat'],
            'raptors': ['toronto raptors'],
            'mavericks': ['dallas mavericks', 'mavs'],
            'nuggets': ['denver nuggets'],
            'hawks': ['atlanta hawks'],
            'nets': ['brooklyn nets'],
            'cavaliers': ['cleveland cavaliers', 'cavs'],
            'rockets': ['houston rockets'],
            'clippers': ['la clippers', 'los angeles clippers'],
            'grizzlies': ['memphis grizzlies'],
            'timberwolves': ['minnesota timberwolves', 'wolves'],
            'pelicans': ['new orleans pelicans'],
            'thunder': ['oklahoma city thunder'],
            'magic': ['orlando magic'],
            'suns': ['phoenix suns'],
            'trail blazers': ['portland trail blazers', 'blazers'],
            'kings': ['sacramento kings'],
            'spurs': ['san antonio spurs'],
            'jazz': ['utah jazz'],
            'wizards': ['washington wizards'],
            'hornets': ['charlotte hornets'],
            'pacers': ['indiana pacers'],
            '76ers': ['philadelphia 76ers', 'sixers'],
            'pistons': ['detroit pistons'],
            'bucks': ['milwaukee bucks'],

            # NFL teams
            'chiefs': ['kansas city chiefs'],
            '49ers': ['san francisco 49ers'],
            'eagles': ['philadelphia eagles'],
            'cowboys': ['dallas cowboys'],
            'ravens': ['baltimore ravens'],
            'packers': ['green bay packers'],
            'bills': ['buffalo bills'],
            'dolphins': ['miami dolphins'],
            'broncos': ['denver broncos'],
            'seahawks': ['seattle seahawks'],
            'steelers': ['pittsburgh steelers'],
            'browns': ['cleveland browns'],
            'texans': ['houston texans'],
            'colts': ['indianapolis colts'],
            'raiders': ['las vegas raiders', 'oakland raiders'],
            'chargers': ['los angeles chargers'],
            'falcons': ['atlanta falcons'],
            'saints': ['new orleans saints'],
            'titans': ['tennessee titans'],
            'commanders': ['washington commanders'],
            'panthers': ['carolina panthers'],
            'jaguars': ['jacksonville jaguars'],
            'cardinals': ['arizona cardinals'],
            'rams': ['los angeles rams'],
            'giants': ['new york giants'],
            'jets': ['new york jets'],
            'lions': ['detroit lions'],
            'vikings': ['minnesota vikings'],
            'patriots': ['new england patriots'],
            'bears': ['chicago bears'],
            'bengals': ['cincinnati bengals'],
            'buccaneers': ['tampa bay buccaneers']
        }

    def extract_teams_enhanced(self, event):
        """ENHANCED: Much better team extraction with common name handling"""
        # Clean the event name first
        clean_event = re.sub(r'[^\w\s@vs\.\-&]', ' ', event)
        clean_event = re.sub(r'\s+', ' ', clean_event).strip()

        # Try different separators
        separators = [' @ ', ' vs ', ' at ', ' - ', ' vs. ']

        for sep in separators:
            if sep in clean_event:
                parts = clean_event.split(sep)
                if len(parts) >= 2:
                    team1 = parts[0].strip().lower()
                    team2 = parts[1].strip().lower()

                    # Expand common team names to full names
                    for short_name, full_names in self.team_variations.items():
                        if team1 == short_name:
                            team1 = full_names[0]  # Use the first full name
                        if team2 == short_name:
                            team2 = full_names[0]

                    return [team1, team2]

        return []

    def teams_match_enhanced(self, bet_teams, game_teams):
        """ENHANCED: Much better team matching with multiple strategies"""
        if not bet_teams or len(bet_teams) != 2:
            return False

        bet_team1, bet_team2 = bet_teams
        game_home = game_teams['home'].lower()
        game_away = game_teams['away'].lower()

        # Strategy 1: Exact matching
        exact_matches = (
                (bet_team1 == game_home and bet_team2 == game_away) or
                (bet_team1 == game_away and bet_team2 == game_home)
        )

        if exact_matches:
            return True

        # Strategy 2: Partial matching (team names contain each other)
        partial_matches = (
                (bet_team1 in game_home and bet_team2 in game_away) or
                (bet_team1 in game_away and bet_team2 in game_home) or
                (game_home in bet_team1 and game_away in bet_team2) or
                (game_home in bet_team2 and game_away in bet_team1)
        )

        if partial_matches:
            return True

        # Strategy 3: Common name mapping
        for short_name, full_names in self.team_variations.items():
            expanded_bet1 = full_names[0] if bet_team1 == short_name else bet_team1
            expanded_bet2 = full_names[0] if bet_team2 == short_name else bet_team2
            expanded_home = full_names[0] if game_home == short_name else game_home
            expanded_away = full_names[0] if game_away == short_name else game_away

            expanded_matches = (
                    (expanded_bet1 == expanded_home and expanded_bet2 == expanded_away) or
                    (expanded_bet1 == expanded_away and expanded_bet2 == expanded_home)
            )

            if expanded_matches:
                return True

        # Strategy 4: Fuzzy matching as final fallback
        try:
            home_match1 = fuzz.partial_ratio(bet_team1, game_home) > 70
            away_match2 = fuzz.partial_ratio(bet_team2, game_away) > 70
            home_match2 = fuzz.partial_ratio(bet_team2, game_home) > 70
            away_match1 = fuzz.partial_ratio(bet_team1, game_away) > 70

            fuzzy_match = (home_match1 and away_match2) or (home_match2 and away_match1)

            if fuzzy_match:
                return True

        except Exception as e:
            print(f"Fuzzy matching error: {e}")

        return False


# =============================================================================
# ULTRA STABLE SCANNER (YOUR VERSION - ENHANCED)
# =============================================================================

class UltraStableScanner:
    def __init__(self):
        self.driver = None
        self.use_demo_mode = False

    def initialize_webdriver(self):
        """Ultra-stable WebDriver initialization with better error handling"""
        try:
            # Close existing driver if any
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
                self.driver = None

            chrome_options = Options()

            # Essential stability options
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")

            # Headless for stability
            chrome_options.add_argument("--headless")

            # Additional stability options
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)

            # Disable extensions and background pages
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-background-timer-throttling")
            chrome_options.add_argument("--disable-backgrounding-occluded-windows")
            chrome_options.add_argument("--disable-renderer-backgrounding")

            # Timeout and performance options
            chrome_options.add_argument("--no-first-run")
            chrome_options.add_argument("--no-default-browser-check")
            chrome_options.add_argument("--disable-component-update")

            driver = webdriver.Chrome(options=chrome_options)
            driver.set_page_load_timeout(config.scanner.page_load_timeout)
            driver.implicitly_wait(config.scanner.implicit_wait)

            print("WebDriver initialized successfully")
            return driver
        except Exception as e:
            print(f"WebDriver initialization failed: {e}")
            return None

    def extract_player_name(self, market: str, event: str) -> str:
        """Extract player name from market description"""
        try:
            # Common patterns in player prop markets
            market_lower = market.lower()

            # Remove common market phrases to isolate player name
            player_market = market

            # Remove market type indicators
            market_indicators = [
                'player', 'receiving yards', 'rushing yards', 'passing yards',
                'touchdowns', 'td', 'over', 'under', 'first', 'last', 'scorer',
                '1st quarter', '2nd quarter', '3rd quarter', '4th quarter',
                'quarter', 'yards', 'yard', 'to score', 'score', 'anytime',
                'first td', 'last td', 'anytime td'
            ]

            for indicator in market_indicators:
                player_market = player_market.replace(indicator, '').replace(indicator.title(), '')

            # Clean up extra spaces and common words
            player_market = re.sub(r'\s+', ' ', player_market).strip()

            # Remove any remaining odds or numbers
            player_market = re.sub(r'[+-]?\d+(?:\.\d+)?', '', player_market).strip()

            # Clean up any remaining special characters
            player_market = re.sub(r'[^\w\s]', '', player_market).strip()

            # If we can't extract a meaningful name, use a placeholder
            if not player_market or len(player_market) < 2:
                return "Player Unknown"

            return player_market.strip()

        except Exception as e:
            print(f"Could not extract player name from '{market}': {e}")
            return "Player Unknown"

    def scrape_crazyninja_safe(self):
        """Safe scraping that falls back to demo data immediately on failure"""
        # Always create a new WebDriver instance for each scan
        self.driver = self.initialize_webdriver()

        if not self.driver:
            print("Using demo mode - WebDriver unavailable")
            return self.generate_realistic_opportunities()

        try:
            print("Opening: https://crazyninjaodds.com/site/tools/positive-ev.aspx")
            self.driver.get("https://crazyninjaodds.com/site/tools/positive-ev.aspx")

            # Wait for page load with generous timeout
            WebDriverWait(self.driver, 30).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )

            print("Betting content loaded successfully")
            time.sleep(3)  # Allow JavaScript to load

            # Try to parse opportunities
            opportunities = self.parse_opportunities_safe()

            # Always close driver after use to prevent session issues
            self.driver.quit()
            self.driver = None

            return opportunities

        except Exception as e:
            print(f"Scraping failed: {e}")
            # Close driver on failure
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
                self.driver = None
            # Immediately fall back to demo data without retry
            return self.generate_realistic_opportunities()

    def parse_opportunities_safe(self):
        """Safe parsing with player name extraction - FIXED DATE PARSING"""
        try:
            if not self.driver:
                return self.generate_realistic_opportunities()

            tables = self.driver.find_elements(By.TAG_NAME, "table")
            print(f"Found {len(tables)} tables")

            if not tables or len(tables) == 0:
                return self.generate_realistic_opportunities()

            opportunities = []
            # Try to parse the main table (usually first one)
            main_table = tables[0]
            rows = main_table.find_elements(By.TAG_NAME, "tr")

            print(f"Found {len(rows)} rows in main table")

            # Process a limited number of rows to avoid timeouts
            max_rows_to_process = min(15, len(rows) - 1)  # Skip header

            for i in range(1, max_rows_to_process + 1):
                try:
                    row = rows[i]
                    cells = row.find_elements(By.TAG_NAME, "td")

                    if len(cells) >= 11:  # Need at least 11 columns for sportsbook
                        # Extract data using correct column indices
                        ev_text = cells[0].text.strip('%')  # LW-WC EV%

                        # ========== CRITICAL: IMPROVED DATE EXTRACTION ==========
                        date_text = cells[3].text if len(cells) > 3 else ''  # Date column
                        sport = cells[4].text if len(cells) > 4 else 'Unknown'  # Sport
                        event = cells[6].text if len(cells) > 6 else 'Unknown'  # Event
                        market = cells[7].text  # "Player First Touchdown Scorer"
                        player_name = cells[8].text  # "CharMar Brown" (ACTUAL PLAYER NAME!)
                        odds = cells[9].text if len(cells) > 9 else 'Unknown'  # Odds
                        sportsbook = cells[10].text if len(cells) > 10 else 'Unknown'  # Sportsbook

                        # ========== NEW: CHECK IF EVENT ALREADY CONTAINS DATE ==========
                        event_date = None

                        # Look for date patterns in the event string
                        date_patterns = [
                            r'(\d{4}-\d{2}-\d{2})',  # YYYY-MM-DD
                            r'(\d{1,2}/\d{1,2}/\d{4})',  # MM/DD/YYYY or DD/MM/YYYY
                            r'(\d{1,2}\.\d{1,2}\.\d{4})',  # DD.MM.YYYY
                            r'(\d{4}\.\d{2}\.\d{2})',  # YYYY.MM.DD
                        ]

                        for pattern in date_patterns:
                            match = re.search(pattern, event)
                            if match:
                                event_date = match.group(1)
                                print(f"DEBUG Found date in event: {event_date}")
                                break

                        # ========== DETERMINE WHICH DATE TO USE ==========
                        if event_date:
                            # Use the date from the event string (most reliable)
                            game_date = self._parse_date_from_string(event_date)
                            print(f"DEBUG Using date from event: {game_date}")
                            # Event already has date, keep as is
                            event_with_date = event
                        elif date_text:
                            # Parse the date column with improved logic
                            game_date = self._parse_website_date(date_text, sport)
                            print(f"DEBUG Using parsed date from column: {game_date}")
                            # Append date to event for R script
                            event_with_date = f"{event} {game_date}"
                        else:
                            # No date found - use tomorrow
                            from datetime import datetime, timedelta
                            game_date = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
                            print(f"DEBUG No date found, using tomorrow: {game_date}")
                            event_with_date = f"{event} {game_date}"

                        # Debug logging
                        print(f"Final game date: {game_date}")
                        print(f"Event with date: {event_with_date}")
                        # ========== END DATE PROCESSING ==========

                        try:
                            ev_value = float(ev_text) / 100
                        except:
                            ev_value = 0.0

                        # Only include valid opportunities (using config threshold)
                        if ev_value >= config.betting.min_ev_threshold and odds and odds != 'Unknown':
                            opportunity = {
                                'ev': ev_value,
                                'sport': sport,
                                'event': event_with_date,  # ← USE EVENT WITH DATE!
                                'market': market,
                                'player': player_name,
                                'odds': odds,
                                'sportsbook': sportsbook,
                                'game_date': game_date,  # ← ADD SEPARATE DATE FIELD
                                'raw_date': date_text,  # ← ADD FOR DEBUGGING
                                'has_event_date': bool(event_date)  # ← TRACK IF DATE FROM EVENT
                            }
                            opportunities.append(opportunity)
                            print(
                                f"Found: {sport} | {event_with_date} | {player_name} - {market} @ {odds} | EV: +{ev_value:.1%} | Book: {sportsbook}")

                except Exception as e:
                    print(f"Error parsing row {i}: {e}")
                    continue  # Skip problematic rows

            if opportunities:
                print(f"Successfully parsed {len(opportunities)} opportunities from website")
                return opportunities
            else:
                print("No valid opportunities found, using demo data")
                return self.generate_realistic_opportunities()

        except Exception as e:
            print(f"Parsing error: {e}")
            return self.generate_realistic_opportunities()

    def _parse_date_from_string(self, date_str: str) -> str:
        """Parse date from string with various formats"""
        from datetime import datetime

        formats = [
            '%Y-%m-%d',  # 2025-12-28
            '%m/%d/%Y',  # 12/28/2025
            '%d/%m/%Y',  # 28/12/2025
            '%d.%m.%Y',  # 28.12.2025
            '%Y.%m.%d',  # 2025.12.28
            '%Y-%m-%d',  # 2025-12-28
        ]

        for fmt in formats:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                return parsed_date.strftime('%Y-%m-%d')
            except ValueError:
                continue

        # If all formats fail, return as-is
        print(f"WARNING: Could not parse date string: {date_str}")
        return date_str

    def _parse_website_date(self, date_text: str, sport: str) -> str:
        """Parse German date formats correctly with YEAR INFERENCE"""
        try:
            # Clean the date text
            date_text = date_text.strip()

            if not date_text:
                from datetime import datetime
                return datetime.now().strftime('%Y-%m-%d')

            from datetime import datetime
            current_year = datetime.now().year
            current_month = datetime.now().month

            # ========== NEW: Check for explicit year in date text ==========
            year_match = re.search(r'(\d{4})', date_text)
            if year_match:
                explicit_year = int(year_match.group(1))
                print(f"DEBUG Found explicit year in date: {explicit_year}")
                current_year = explicit_year  # Use the year from the date text!

            # Check German date formats
            german_month_map = {
                'jan': '01', 'januar': '01',
                'feb': '02', 'februar': '02',
                'mär': '03', 'märz': '03',
                'apr': '04', 'april': '04',
                'mai': '05', 'mai': '05',
                'jun': '06', 'juni': '06',
                'jul': '07', 'juli': '07',
                'aug': '08', 'august': '08',
                'sep': '09', 'september': '09',
                'okt': '10', 'oktober': '10',
                'nov': '11', 'november': '11',
                'dez': '12', 'dezember': '12'
            }

            # Debug: Print what we're trying to parse
            print(f"DEBUG Parsing date: '{date_text}' (current: {current_year}-{current_month:02d})")

            # ========== TRY GERMAN FORMATS ==========
            # Try German format: "So., 14. Dez. at 22:25"
            german_match = re.search(r'(\d{1,2})\.\s+(\w+)\.', date_text, re.IGNORECASE)
            if german_match:
                day = german_match.group(1)
                month_word = german_match.group(2).lower()[:3]  # First 3 chars

                if month_word in german_month_map:
                    month = german_month_map[month_word]
                    month_num = int(month)

                    # ========== CRITICAL: INFER CORRECT YEAR ==========
                    year = self._infer_correct_year(month_num, current_year, current_month)

                    result = f"{year}-{month}-{day.zfill(2)}"
                    print(f"DEBUG German date parsed: {date_text} -> {result} (inferred year: {year})")
                    return result

            # Try format with weekday: "So., 14. Dez."
            german_match2 = re.search(r'\w+\.\s+(\d{1,2})\.\s+(\w+)\.', date_text, re.IGNORECASE)
            if german_match2:
                day = german_match2.group(1)
                month_word = german_match2.group(2).lower()[:3]

                if month_word in german_month_map:
                    month = german_month_map[month_word]
                    month_num = int(month)

                    # ========== CRITICAL: INFER CORRECT YEAR ==========
                    year = self._infer_correct_year(month_num, current_year, current_month)

                    result = f"{year}-{month}-{day.zfill(2)}"
                    print(f"DEBUG German weekday date parsed: {date_text} -> {result} (inferred year: {year})")
                    return result

            # ========== TRY ENGLISH FORMATS ==========
            # Try English format: "Dec 14" or "Dec 14 at 22:25"
            english_match = re.search(r'(\w+)\s+(\d{1,2})', date_text, re.IGNORECASE)
            if english_match:
                month_word = english_match.group(1).lower()[:3]
                day = english_match.group(2)

                english_month_map = {
                    'jan': '01', 'january': '01',
                    'feb': '02', 'february': '02',
                    'mar': '03', 'march': '03',
                    'apr': '04', 'april': '04',
                    'may': '05', 'may': '05',
                    'jun': '06', 'june': '06',
                    'jul': '07', 'july': '07',
                    'aug': '08', 'august': '08',
                    'sep': '09', 'september': '09',
                    'oct': '10', 'october': '10',
                    'nov': '11', 'november': '11',
                    'dec': '12', 'december': '12'
                }

                if month_word in english_month_map:
                    month = english_month_map[month_word]
                    month_num = int(month)

                    # ========== CRITICAL: INFER CORRECT YEAR ==========
                    year = self._infer_correct_year(month_num, current_year, current_month)

                    result = f"{year}-{month}-{day.zfill(2)}"
                    print(f"DEBUG English date parsed: {date_text} -> {result} (inferred year: {year})")
                    return result

            # ========== TRY NUMERIC FORMATS ==========
            # Try MM/DD format
            if '/' in date_text:
                parts = date_text.split('/')
                if len(parts) == 2:
                    try:
                        month = int(parts[0])
                        day = int(parts[1])

                        # ========== CRITICAL: INFER CORRECT YEAR ==========
                        year = self._infer_correct_year(month, current_year, current_month)

                        result = f"{year}-{month:02d}-{day:02d}"
                        print(f"DEBUG MM/DD date parsed: {date_text} -> {result} (inferred year: {year})")
                        return result
                    except:
                        pass
                elif len(parts) == 3:
                    # Could be MM/DD/YYYY or DD/MM/YYYY
                    try:
                        # Try MM/DD/YYYY
                        month = int(parts[0])
                        day = int(parts[1])
                        year = int(parts[2])
                        result = f"{year}-{month:02d}-{day:02d}"
                        print(f"DEBUG MM/DD/YYYY date parsed: {date_text} -> {result}")
                        return result
                    except:
                        # Try DD/MM/YYYY
                        try:
                            day = int(parts[0])
                            month = int(parts[1])
                            year = int(parts[2])
                            result = f"{year}-{month:02d}-{day:02d}"
                            print(f"DEBUG DD/MM/YYYY date parsed: {date_text} -> {result}")
                            return result
                        except:
                            pass

            # ========== TRY DD.MM.YYYY FORMAT ==========
            if '.' in date_text and re.search(r'\d{1,2}\.\d{1,2}\.\d{4}', date_text):
                try:
                    parts = date_text.split('.')
                    if len(parts) >= 3:
                        day = int(parts[0])
                        month = int(parts[1])
                        year = int(parts[2])
                        result = f"{year}-{month:02d}-{day:02d}"
                        print(f"DEBUG DD.MM.YYYY date parsed: {date_text} -> {result}")
                        return result
                except:
                    pass

            # ========== CHECK FOR "TOMORROW" OR "TODAY" ==========
            date_lower = date_text.lower()
            from datetime import datetime, timedelta

            if 'tomorrow' in date_lower:
                tomorrow = datetime.now() + timedelta(days=1)
                result = tomorrow.strftime('%Y-%m-%d')
                print(f"DEBUG 'Tomorrow' parsed: {date_text} -> {result}")
                return result
            elif 'today' in date_lower or 'tonight' in date_lower:
                result = datetime.now().strftime('%Y-%m-%d')
                print(f"DEBUG 'Today' parsed: {date_text} -> {result}")
                return result

            # If all parsing fails, use tomorrow (since games are likely future dates)
            tomorrow = datetime.now() + timedelta(days=1)
            result = tomorrow.strftime('%Y-%m-%d')
            print(f"Could not parse date '{date_text}', defaulting to: {result}")
            return result

        except Exception as e:
            print(f"Date parsing error: {e}")
            # Default to tomorrow for future games
            from datetime import datetime, timedelta
            tomorrow = datetime.now() + timedelta(days=1)
            return tomorrow.strftime('%Y-%m-%d')

    def _infer_correct_year(self, month_num: int, current_year: int, current_month: int) -> int:
        """
        Infer the correct year for a date based on month and current date.
        This handles year boundaries (e.g., December games in January).
        """
        # If we already found an explicit year in the date text, use it
        # (This is handled in the calling method)

        # Rule 1: If month is December (12) and current month is Jan-Jun, it's previous year
        if month_num == 12 and current_month <= 6:
            print(f"DEBUG Year inference: December game in Jan-Jun -> previous year")
            return current_year - 1

        # Rule 2: If month is January (1) and current month is Dec, it's next year
        if month_num == 1 and current_month == 12:
            print(f"DEBUG Year inference: January game in December -> next year")
            return current_year + 1

        # Rule 3: If month is earlier in year than current month (and not crossing year boundary)
        # Could be next year if we're near end of year
        if month_num < current_month:
            # Check if we might be looking at next year's schedule
            # If current month is Oct-Dec and game month is Jan-Mar, it's likely next year
            if current_month >= 10 and month_num <= 3:
                print(f"DEBUG Year inference: Early month {month_num} in late year {current_month} -> next year")
                return current_year + 1
            else:
                print(f"DEBUG Year inference: Month {month_num} < current month {current_month} -> current year")
                return current_year

        # Default: Use current year
        print(f"DEBUG Year inference: Using current year {current_year}")
        return current_year


    def generate_realistic_opportunities(self):
        """Generate realistic +EV opportunities with player names"""
        print("GENERATING REALISTIC +EV OPPORTUNITIES")

        # Mix of real current games with player names
        events = [
            {"sport": "NFL", "event": "Bengals @ Ravens", "market": "Ja'Marr Chase Receiving Yards",
             "player": "Ja'Marr Chase", "odds": "+185", "ev": 0.085, "sportsbook": "DraftKings"},
            {"sport": "NFL", "event": "Packers @ Lions", "market": "Jared Goff Passing Yards",
             "player": "Jared Goff", "odds": "-115", "ev": 0.042, "sportsbook": "FanDuel"},
            {"sport": "NBA", "event": "Lakers @ Warriors", "market": "LeBron James Points",
             "player": "LeBron James", "odds": "-110", "ev": 0.045, "sportsbook": "BetMGM"},
            {"sport": "NFL", "event": "Bears @ Eagles", "market": "Jalen Hurts Rushing Yards",
             "player": "Jalen Hurts", "odds": "+120", "ev": 0.038, "sportsbook": "Caesars"},
            {"sport": "NBA", "event": "Celtics @ Heat", "market": "Jimmy Butler Assists",
             "player": "Jimmy Butler", "odds": "-105", "ev": 0.035, "sportsbook": "William Hill"},
            {"sport": "NFL", "event": "Chiefs @ Cowboys", "market": "Travis Kelce Receptions",
             "player": "Travis Kelce", "odds": "+150", "ev": 0.052, "sportsbook": "DraftKings"},
        ]

        for event in events:
            print(
                f"Realistic: {event['event']} | {event['player']} - {event['market']} @ {event['odds']} | EV: +{event['ev']:.1%}")

        return events

    def scrape_crazyninja_odds(self) -> List[Dict]:
        """Main scraping method - always returns opportunities"""
        return self.scrape_crazyninja_safe()

    def close(self):
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass


# =============================================================================
# SUPABASE MANAGER (YOUR VERSION - ENHANCED)
# =============================================================================

class SupabaseManager:
    def __init__(self):
        if not SUPABASE_AVAILABLE:
            raise ImportError("Supabase not available. Install with: pip install supabase")

        self.supabase_url = config.database.supabase_url
        self.supabase_key = config.database.supabase_key
        self.supabase: Client = create_client(self.supabase_url, self.supabase_key)
        print("Connected to Supabase!")

    def save_bet(self, bet_data: Dict) -> str:
        """Save a new bet to Supabase - matches your database schema"""
        bet_id = str(uuid.uuid4())

        bet_record = {
            'id': bet_id,
            'event': bet_data['event'],
            'sport': bet_data['sport'],
            'market': bet_data.get('market', ''),
            'player': bet_data.get('player', 'Unknown'),
            'odds': float(bet_data['odds']),
            'stake': float(bet_data['stake']),
            'potential_win': float(bet_data['stake'] * bet_data['odds']),
            'ev': float(bet_data['ev']),
            'sportsbook': bet_data['sportsbook'],
            'status': 'pending',
            'kelly_percent': float(bet_data.get('kelly_percent', 0)),
            'edge_percent': float(bet_data.get('edge_percent', 0)),
            'placed_at': datetime.now().isoformat(),
            'profit': 0.0,
            'game_date': bet_data.get('game_date'),  # NEW: Add game date
            'resolution_state': 'pending',  # For IdempotentResolutionManager
            'resolution_error': None,
            'last_resolution_attempt': None
        }

        try:
            response = self.supabase.table('bets').insert(bet_record).execute()
            if hasattr(response, 'error') and response.error:
                print(f"Error saving bet: {response.error}")
                return None
            print(f"Bet saved to Supabase: {bet_data['event']}")
            return bet_id
        except Exception as e:
            print(f"Exception saving bet: {e}")
            return None

    def get_bet(self, bet_id: str) -> Optional[Dict]:
        """Get a single bet by ID with proper field mapping"""
        try:
            response = self.supabase.table('bets').select('*').eq('id', bet_id).execute()

            if response.data and len(response.data) > 0:
                bet = response.data[0]

                # Map database columns to Python expected names
                return {
                    'id': bet['id'],
                    'player': bet.get('player'),  # Actual column: 'player'
                    'player_name': bet.get('player'),  # Alias for compatibility
                    'sport': bet.get('sport'),
                    'market': bet.get('market'),  # Actual column: 'market'
                    'market_type': bet.get('market'),  # Alias for compatibility
                    'event': bet.get('event'),  # Actual column: 'event'
                    'event_string': bet.get('event'),  # Alias for compatibility
                    'game_date': bet.get('game_date'),
                    'stake': bet.get('stake'),
                    'odds': bet.get('odds'),
                    'line_value': None,  # Doesn't exist in DB, will extract from market
                    'season': None,  # Doesn't exist in DB, will calculate from game_date
                    'status': bet.get('status'),
                    'profit': bet.get('profit'),
                    'result': bet.get('result')
                }
            return None
        except Exception as e:
            print(f"Error getting bet: {e}")
            return None

    def update_bet_result(self, bet_id: str, won: bool, profit: float):
        """Update bet result in Supabase"""
        update_data = {
            'status': 'won' if won else 'lost',
            'result': 'win' if won else 'loss',
            'profit': float(profit),
            'resolved_at': datetime.now().isoformat(),
            'resolution_state': 'complete'
        }

        try:
            response = self.supabase.table('bets').update(update_data).eq('id', bet_id).execute()
            print(f"Bet {bet_id} updated: {'won' if won else 'lost'} (€{profit:.2f})")
        except Exception as e:
            print(f"Error updating bet: {e}")

    def update_bet_error(self, bet_id: str, error: str):
        """Update bet with error information"""
        update_data = {
            'resolution_error': error,
            'last_resolution_attempt': datetime.now().isoformat()
        }

        try:
            response = self.supabase.table('bets').update(update_data).eq('id', bet_id).execute()
            print(f"Updated bet {bet_id} error: {error[:50]}...")
        except Exception as e:
            print(f"Error updating bet error: {e}")

    def update_bet_state(self, bet_id: str, state: str, note: str = ""):
        """Update bet resolution state"""
        update_data = {
            'resolution_state': state,
            'last_resolution_attempt': datetime.now().isoformat()
        }

        if note:
            update_data['resolution_note'] = note

        try:
            response = self.supabase.table('bets').update(update_data).eq('id', bet_id).execute()
            print(f"Updated bet {bet_id} state to {state}: {note}")
        except Exception as e:
            print(f"Error updating bet state: {e}")

    def get_pending_bets(self, limit: int = None) -> List[Dict]:
        """Get all pending bets from Supabase"""
        try:
            query = self.supabase.table('bets').select('*').eq('status', 'pending')
            if limit:
                query = query.limit(limit)
            response = query.execute()
            return response.data if response.data else []
        except Exception as e:
            print(f"Error fetching pending bets: {e}")
            return []

    def get_bet_history(self, limit: int = 100) -> List[Dict]:
        """Get bet history from Supabase"""
        try:
            response = self.supabase.table('bets').select('*').order('placed_at', desc=True).limit(limit).execute()
            return response.data if response.data else []
        except Exception as e:
            print(f"Error fetching bet history: {e}")
            return []

    def save_performance_metrics(self, bankroll: float):
        """Save performance metrics to Supabase"""
        # Get current stats from all bets
        bets_response = self.supabase.table('bets').select('*').execute()
        all_bets = bets_response.data if bets_response.data else []

        total_bets = len(all_bets)
        won_bets = len([b for b in all_bets if b.get('status') == 'won'])
        lost_bets = len([b for b in all_bets if b.get('status') == 'lost'])
        pending_bets = len([b for b in all_bets if b.get('status') == 'pending'])
        total_profit = sum(b.get('profit', 0) for b in all_bets)
        total_wagered = sum(b.get('stake', 0) for b in all_bets)

        win_rate = (won_bets / (won_bets + lost_bets)) * 100 if (won_bets + lost_bets) > 0 else 0

        metrics_record = {
            'id': str(uuid.uuid4()),
            'bankroll': float(bankroll),
            'total_bets': total_bets,
            'won_bets': won_bets,
            'lost_bets': lost_bets,
            'pending_bets': pending_bets,
            'total_profit': float(total_profit),
            'total_wagered': float(total_wagered),
            'win_rate': float(win_rate),
            'timestamp': datetime.now().isoformat()
        }

        try:
            response = self.supabase.table('performance_metrics').insert(metrics_record).execute()
            print("Performance metrics saved to Supabase")
        except Exception as e:
            print(f"Error saving metrics: {e}")

    def get_performance_stats(self) -> Dict:
        """Get comprehensive performance statistics from Supabase"""
        try:
            bets_response = self.supabase.table('bets').select('*').execute()
            all_bets = bets_response.data if bets_response.data else []

            total_bets = len(all_bets)
            won_bets = len([b for b in all_bets if b.get('status') == 'won'])
            lost_bets = len([b for b in all_bets if b.get('status') == 'lost'])
            pending_bets = len([b for b in all_bets if b.get('status') == 'pending'])
            total_profit = sum(b.get('profit', 0) for b in all_bets)
            total_wagered = sum(b.get('stake', 0) for b in all_bets)

            win_rate = (won_bets / (won_bets + lost_bets)) * 100 if (won_bets + lost_bets) > 0 else 0

            return {
                'total_bets': total_bets,
                'won_bets': won_bets,
                'lost_bets': lost_bets,
                'pending_bets': pending_bets,
                'total_profit': total_profit,
                'total_wagered': total_wagered,
                'win_rate': win_rate
            }
        except Exception as e:
            print(f"Error getting performance stats: {e}")
            return {
                'total_bets': 0,
                'won_bets': 0,
                'lost_bets': 0,
                'pending_bets': 0,
                'total_profit': 0,
                'total_wagered': 0,
                'win_rate': 0
            }


# =============================================================================
# SQLITE DATABASE MANAGER (FOR BACKWARD COMPATIBILITY)
# =============================================================================

class DatabaseManager:
    def __init__(self, db_path: str = "sports_betting.db"):
        self.db_path = db_path
        self.init_database()

    def init_database(self):
        """Initialize database with required tables"""
        conn = self.get_connection()
        try:
            # Bets table - UPDATED WITH RESOLUTION STATE COLUMNS
            conn.execute('''
                CREATE TABLE IF NOT EXISTS bets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event TEXT NOT NULL,
                    sport TEXT NOT NULL,
                    market TEXT NOT NULL,
                    player TEXT NOT NULL,
                    odds REAL NOT NULL,
                    stake REAL NOT NULL,
                    potential_win REAL NOT NULL,
                    ev REAL NOT NULL,
                    sportsbook TEXT NOT NULL,
                    status TEXT DEFAULT 'pending',
                    placed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    resolved_at TIMESTAMP NULL,
                    result TEXT NULL,
                    profit REAL DEFAULT 0,
                    kelly_percent REAL NULL,
                    edge_percent REAL NULL,
                    game_date TEXT NULL,
                    resolution_state TEXT DEFAULT 'pending',
                    resolution_error TEXT NULL,
                    last_resolution_attempt TIMESTAMP NULL,
                    resolution_note TEXT NULL
                )
            ''')

            # Performance metrics table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS performance_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    bankroll REAL NOT NULL,
                    total_bets INTEGER DEFAULT 0,
                    won_bets INTEGER DEFAULT 0,
                    lost_bets INTEGER DEFAULT 0,
                    pending_bets INTEGER DEFAULT 0,
                    total_profit REAL DEFAULT 0,
                    total_wagered REAL DEFAULT 0,
                    win_rate REAL DEFAULT 0
                )
            ''')

            conn.commit()
            print("SQLite Database initialized successfully")

        except Exception as e:
            print(f"Error initializing database: {e}")
            raise
        finally:
            conn.close()

    def get_connection(self):
        """Get database connection with row factory"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def save_bet(self, bet_data: Dict) -> int:
        """Save a new bet and return its ID"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO bets 
                (event, sport, market, player, odds, stake, potential_win, ev, sportsbook, 
                 status, placed_at, kelly_percent, edge_percent, game_date, resolution_state)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                bet_data['event'],
                bet_data['sport'],
                bet_data.get('market', ''),
                bet_data.get('player', 'Unknown'),
                bet_data['odds'],
                bet_data['stake'],
                bet_data['potential_win'],
                bet_data['ev'],
                bet_data['sportsbook'],
                bet_data.get('status', 'pending'),
                bet_data.get('placed_at', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                bet_data.get('kelly_percent'),
                bet_data.get('edge_percent'),
                bet_data.get('game_date'),
                'pending'  # Initial resolution state
            ))
            bet_id = cursor.lastrowid
            conn.commit()
            print(f"Bet saved with ID: {bet_id}")
            return bet_id
        except Exception as e:
            print(f"Error saving bet: {e}")
            raise
        finally:
            conn.close()

    def get_bet(self, bet_id: int) -> Optional[Dict]:
        """Get a single bet by ID"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM bets WHERE id = ?", (bet_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
        except Exception as e:
            print(f"Error getting bet {bet_id}: {e}")
            return None
        finally:
            conn.close()

    def update_bet_result(self, bet_id: int, won: bool, profit: float):
        """Update bet result"""
        conn = self.get_connection()
        try:
            conn.execute('''
                UPDATE bets 
                SET status = ?, result = ?, profit = ?, resolved_at = ?, resolution_state = ?
                WHERE id = ?
            ''', (
                'won' if won else 'lost',
                'win' if won else 'loss',
                profit,
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'complete',
                bet_id
            ))
            conn.commit()
            print(f"Bet {bet_id} updated: {'won' if won else 'lost'}")
        except Exception as e:
            print(f"Error updating bet {bet_id}: {e}")
            raise
        finally:
            conn.close()

    def update_bet_error(self, bet_id: int, error: str):
        """Update bet with error information"""
        conn = self.get_connection()
        try:
            conn.execute('''
                UPDATE bets 
                SET resolution_error = ?, last_resolution_attempt = ?
                WHERE id = ?
            ''', (
                error,
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                bet_id
            ))
            conn.commit()
            print(f"Updated bet {bet_id} error: {error[:50]}...")
        except Exception as e:
            print(f"Error updating bet error: {e}")
        finally:
            conn.close()

    def update_bet_state(self, bet_id: int, state: str, note: str = ""):
        """Update bet resolution state"""
        conn = self.get_connection()
        try:
            update_data = {
                'resolution_state': state,
                'last_resolution_attempt': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            if note:
                update_data['resolution_note'] = note

            # Build update query
            set_clause = ', '.join(f"{key} = ?" for key in update_data.keys())
            values = list(update_data.values()) + [bet_id]

            conn.execute(f'''
                UPDATE bets 
                SET {set_clause}
                WHERE id = ?
            ''', values)
            conn.commit()
            print(f"Updated bet {bet_id} state to {state}: {note}")
        except Exception as e:
            print(f"Error updating bet state: {e}")
        finally:
            conn.close()

    def get_pending_bets(self, limit: int = None) -> List[Dict]:
        """Get all pending bets"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            query = "SELECT * FROM bets WHERE status = 'pending'"
            if limit:
                query += f" LIMIT {limit}"
            cursor.execute(query)
            return [dict(row) for row in cursor.fetchall()]
        finally:
            conn.close()

    def get_bet_history(self, limit: int = 100) -> List[Dict]:
        """Get bet history"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM bets ORDER BY placed_at DESC LIMIT ?", (limit,))
            return [dict(row) for row in cursor.fetchall()]
        finally:
            conn.close()

    def save_performance_metrics(self, metrics: Dict):
        """Save performance metrics snapshot"""
        conn = self.get_connection()
        try:
            conn.execute('''
                INSERT INTO performance_metrics 
                (bankroll, total_bets, won_bets, lost_bets, pending_bets, total_profit, total_wagered, win_rate)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                metrics['bankroll'],
                metrics['total_bets'],
                metrics['won_bets'],
                metrics['lost_bets'],
                metrics['pending_bets'],
                metrics['total_profit'],
                metrics['total_wagered'],
                metrics['win_rate']
            ))
            conn.commit()
        except Exception as e:
            print(f"Error saving metrics: {e}")
            raise
        finally:
            conn.close()

    def get_performance_stats(self) -> Dict:
        """Get comprehensive performance statistics"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()

            # Basic stats
            cursor.execute('''
                SELECT 
                    COUNT(*) as total_bets,
                    SUM(CASE WHEN status = 'won' THEN 1 ELSE 0 END) as won_bets,
                    SUM(CASE WHEN status = 'lost' THEN 1 ELSE 0 END) as lost_bets,
                    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending_bets,
                    SUM(profit) as total_profit,
                    SUM(stake) as total_wagered
                FROM bets
            ''')
            result = cursor.fetchone()
            if result:
                stats = dict(result)
            else:
                stats = {
                    'total_bets': 0,
                    'won_bets': 0,
                    'lost_bets': 0,
                    'pending_bets': 0,
                    'total_profit': 0,
                    'total_wagered': 0
                }

            # Calculate win rate
            if stats['won_bets'] + stats['lost_bets'] > 0:
                stats['win_rate'] = (stats['won_bets'] / (stats['won_bets'] + stats['lost_bets'])) * 100
            else:
                stats['win_rate'] = 0

            return stats
        except Exception as e:
            print(f"Error getting performance stats: {e}")
            return {
                'total_bets': 0,
                'won_bets': 0,
                'lost_bets': 0,
                'pending_bets': 0,
                'total_profit': 0,
                'total_wagered': 0,
                'win_rate': 0
            }
        finally:
            conn.close()


# =============================================================================
# CONFIGURATION SYSTEM
# =============================================================================

@dataclass
class DatabaseConfig:
    db_path: str = "sports_betting.db"
    backup_interval: int = 3600
    use_supabase: bool = False
    supabase_url: str = "https://wdbvrhloznteahaqldoq.supabase.co"
    supabase_key: str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndkYnZyaGxvem50ZWFoYXFsZG9xIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjQ1NTQ4OTYsImV4cCI6MjA4MDEzMDg5Nn0.uKajufRaRa3-XmEuUnfbGdSDujalK3Hi8tJU34QCa9c"


@dataclass
class APIConfig:
    mrdoge_api_key: str = ""
    base_url: str = "https://api.mrdoge.co/v2"
    timeout: int = 30
    max_retries: int = 3
    rate_limit_delay: float = 1.0


@dataclass
class BankrollConfig:
    initial_bankroll: float = 3000.0
    min_stake_percent: float = 0.0
    max_stake_percent: float = 0.10
    max_total_stake_percent: float = 0.25
    max_kelly_fraction: float = 1.0


@dataclass
class ScannerConfig:
    scan_interval: int = 300
    max_scans_per_session: int = 50
    headless: bool = True
    page_load_timeout: int = 45
    implicit_wait: int = 15


@dataclass
class BettingConfig:
    min_ev_threshold: float = 0.02
    max_bets_per_scan: int = 5
    duplicate_cooldown_hours: int = 2


@dataclass
class LoggingConfig:
    level: str = "INFO"
    file: str = "sports_betting.log"
    max_file_size: int = 10 * 1024 * 1024
    backup_count: int = 5


class ConfigManager:
    def __init__(self, config_file: str = "config.json"):
        self.config_file = config_file
        self.database = DatabaseConfig()
        self.api = APIConfig()
        self.bankroll = BankrollConfig()
        self.scanner = ScannerConfig()
        self.betting = BettingConfig()
        self.logging = LoggingConfig()

        self._load_from_env()
        self._load_from_file()
        self._setup_logging()

    def _load_from_env(self):
        """Load configuration from environment variables"""
        # API Configuration
        api_key = os.getenv('MRDOGE_API_KEY')
        if api_key:
            self.api.mrdoge_api_key = api_key

        # Database Configuration
        db_path = os.getenv('DATABASE_PATH')
        if db_path:
            self.database.db_path = db_path

        # Supabase Configuration
        supabase_url = os.getenv('SUPABASE_URL')
        if supabase_url:
            self.database.supabase_url = supabase_url

        supabase_key = os.getenv('SUPABASE_KEY')
        if supabase_key:
            self.database.supabase_key = supabase_key

        # Bankroll Configuration
        initial_br = os.getenv('INITIAL_BANKROLL')
        if initial_br:
            self.bankroll.initial_bankroll = float(initial_br)

    def _load_from_file(self):
        """Load configuration from JSON file"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    config_data = json.load(f)

                # Load each section
                if 'database' in config_data:
                    self._update_from_dict(self.database, config_data['database'])
                if 'api' in config_data:
                    self._update_from_dict(self.api, config_data['api'])
                if 'bankroll' in config_data:
                    self._update_from_dict(self.bankroll, config_data['bankroll'])
                if 'scanner' in config_data:
                    self._update_from_dict(self.scanner, config_data['scanner'])
                if 'betting' in config_data:
                    self._update_from_dict(self.betting, config_data['betting'])
                if 'logging' in config_data:
                    self._update_from_dict(self.logging, config_data['logging'])

                print(f"Configuration loaded from {self.config_file}")
        except Exception as e:
            print(f"Could not load config file: {e}")

    def _update_from_dict(self, config_obj, config_dict: Dict[str, any]):
        """Update a configuration object from dictionary"""
        for key, value in config_dict.items():
            if hasattr(config_obj, key):
                setattr(config_obj, key, value)

    def _setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=getattr(logging, self.logging.level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.logging.file),
                logging.StreamHandler()
            ]
        )

    def validate(self) -> bool:
        """Validate configuration"""
        issues = []

        if not self.api.mrdoge_api_key:
            issues.append("MRDOGE_API_KEY is required")

        if self.database.use_supabase and not SUPABASE_AVAILABLE:
            issues.append("Supabase enabled but supabase package not installed. Run: pip install supabase")

        if self.database.use_supabase and not self.database.supabase_key:
            issues.append("SUPABASE_KEY is required when using Supabase")

        if self.bankroll.initial_bankroll <= 0:
            issues.append("Initial bankroll must be positive")

        if self.betting.min_ev_threshold < 0:
            issues.append("Minimum EV threshold cannot be negative")

        if issues:
            print(f"Configuration validation failed: {', '.join(issues)}")
            return False

        return True


# Global configuration instance
config = ConfigManager()


# =============================================================================
# DATABASE FACTORY
# =============================================================================

def create_database_manager():
    """Factory function to create the appropriate database manager"""
    if config.database.use_supabase:
        if not SUPABASE_AVAILABLE:
            print("Supabase requested but not available. Falling back to SQLite.")
            return DatabaseManager()
        try:
            return SupabaseManager()
        except Exception as e:
            print(f"Failed to connect to Supabase: {e}. Falling back to SQLite.")
            return DatabaseManager()
    else:
        return DatabaseManager()


# =============================================================================
# CRAZY NINJA ODDS PARSER
# =============================================================================

class CrazyNinjaOddsParser:
    @staticmethod
    def parse_crazyninja_odds(odds_text):
        """
        Specifically designed for CrazyNinjaOdds format where odds might be
        in market descriptions like "Player Touchdowns", "Player Rushing Yards"
        """
        if not odds_text:
            return None

        cleaned = str(odds_text).strip()

        # First try to extract any obvious odds patterns
        # Look for American odds: +150, -125
        american_match = re.search(r'([+-]\d+)', cleaned)
        if american_match:
            american_odds = int(american_match.group(1))
            if american_odds > 0:
                return round(american_odds / 100 + 1, 2)
            else:
                return round(100 / abs(american_odds) + 1, 2)

        # Look for decimal odds: 2.50, 1.80
        decimal_match = re.search(r'(\d+\.\d{2})', cleaned)
        if decimal_match:
            return float(decimal_match.group(1))

        # Look for fractional odds: 5/2, 3/1
        fractional_match = re.search(r'(\d+)/(\d+)', cleaned)
        if fractional_match:
            numerator = float(fractional_match.group(1))
            denominator = float(fractional_match.group(2))
            return round((numerator / denominator) + 1, 2)

        # For market types without explicit odds, use reasonable defaults
        market_types = {
            'touchdown': 2.50, 'td': 2.50, 'points': 1.90, 'yards': 1.90,
            'rushing': 1.90, 'receiving': 1.90, 'passing': 1.90,
            'over': 1.90, 'under': 1.90, 'moneyline': 2.00
        }

        cleaned_lower = cleaned.lower()
        for market_key, default_odds in market_types.items():
            if market_key in cleaned_lower:
                return default_odds

        return 1.90  # Default fallback


# =============================================================================
# ENHANCED DUPLICATE DETECTOR
# =============================================================================

class EnhancedDuplicateDetector:
    def __init__(self):
        self.placed_bets_tracker = set()
        self.bet_time_threshold = timedelta(hours=2)

    def is_duplicate_bet(self, event: str, market: str, odds: float, timestamp=None) -> bool:
        """Enhanced duplicate detection with time-based filtering"""
        current_time = datetime.now()

        # Create a temporary set to check recent bets
        recent_bets = set()

        for bet_key in self.placed_bets_tracker:
            parts = bet_key.split('_')
            if len(parts) >= 4:  # event_market_odds_timestamp
                bet_time = datetime.strptime(parts[3], '%Y%m%d%H%M%S')
                # Only consider bets within the time threshold
                if (current_time - bet_time) < self.bet_time_threshold:
                    recent_bets.add(bet_key)

        # Check against recent bets only
        for existing_key in recent_bets:
            existing_parts = existing_key.split('_')
            if len(existing_parts) >= 3:
                existing_event, existing_market, existing_odds = existing_parts[0], existing_parts[1], float(
                    existing_parts[2])

                # Check event similarity
                event_similarity = fuzz.ratio(event.lower(), existing_event.lower())

                # Check market similarity
                market_similarity = fuzz.ratio(market.lower(), existing_market.lower())

                # Check odds similarity (allow small differences)
                odds_similarity = abs(odds - existing_odds) < 0.2

                if event_similarity > 80 and market_similarity > 80 and odds_similarity:
                    return True
        return False

    def add_bet(self, event: str, market: str, odds: float):
        """Add a bet to the tracker with timestamp"""
        bet_key = f"{event}_{market}_{odds:.2f}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        self.placed_bets_tracker.add(bet_key)

        # Clean up old bets
        self.clean_old_bets()

    def clean_old_bets(self):
        """Remove bets older than the time threshold"""
        current_time = datetime.now()
        to_remove = set()

        for bet_key in self.placed_bets_tracker:
            parts = bet_key.split('_')
            if len(parts) >= 4:
                bet_time = datetime.strptime(parts[3], '%Y%m%d%H%M%S')
                if (current_time - bet_time) >= self.bet_time_threshold:
                    to_remove.add(bet_key)

        self.placed_bets_tracker -= to_remove


# =============================================================================
# KELLY CRITERION BANKROLL MANAGEMENT
# =============================================================================

class KellyBankrollManager:
    def __init__(self):
        # Use configuration values
        self.max_kelly_fraction = config.bankroll.max_kelly_fraction
        self.min_stake_percent = config.bankroll.min_stake_percent
        self.max_stake_percent = config.bankroll.max_stake_percent
        self.max_total_stake_percent = config.bankroll.max_total_stake_percent

    def calculate_stake(self, ev_percentage: float, bankroll: float, pending_bets: list, odds: float) -> float:
        """
        Calculate optimal stake using Kelly Criterion:
        f* = (bp - q) / b
        where:
        f* = fraction of bankroll to bet
        b = odds - 1 (net odds received on the bet)
        p = probability of winning
        q = probability of losing = 1 - p
        """

        if ev_percentage < config.betting.min_ev_threshold or odds <= 1.0:
            return 0.0

        try:
            # Calculate implied probability from odds
            implied_prob = 1.0 / odds

            # Calculate actual probability (implied probability + EV edge)
            actual_prob = implied_prob * (1.0 + ev_percentage)

            # Ensure probabilities are valid
            if actual_prob <= 0 or actual_prob >= 1.0:
                return 0.0

            # Kelly Criterion formula
            b = odds - 1.0  # net odds
            p = actual_prob
            q = 1.0 - actual_prob

            # Full Kelly fraction
            full_kelly = (b * p - q) / b

            # Safety checks - must be positive edge
            if full_kelly <= 0:
                return 0.0

            # Use fractional Kelly for safety (more conservative)
            kelly_fraction = full_kelly * self.max_kelly_fraction

            # Calculate stake as percentage of bankroll
            stake_percent = kelly_fraction

            # Apply minimum and maximum limits
            stake_percent = max(stake_percent, self.min_stake_percent)
            stake_percent = min(stake_percent, self.max_stake_percent)

            # Check total commitment limits
            current_pending = sum(float(bet['stake']) for bet in pending_bets)
            current_committed_percent = current_pending / bankroll if bankroll > 0 else 0

            if current_committed_percent >= self.max_total_stake_percent:
                # Reduce stake if over-committed
                available_percent = self.max_total_stake_percent - current_committed_percent
                stake_percent = min(stake_percent, available_percent * 0.5)

            # Calculate actual stake amount
            stake = bankroll * stake_percent

            # Final validation
            if stake < (bankroll * self.min_stake_percent * 0.1):  # Very small stakes
                return 0.0

            return round(stake, 2)

        except Exception as e:
            print(f"Kelly calculation error: {e}")
            return 0.0

    def get_kelly_calculation_details(self, ev_percentage: float, odds: float, bankroll: float) -> dict:
        """Get detailed Kelly calculation info"""
        if ev_percentage < config.betting.min_ev_threshold or odds <= 1.0:
            return {}

        implied_prob = 1.0 / odds
        actual_prob = implied_prob * (1.0 + ev_percentage)
        b = odds - 1.0
        p = actual_prob
        q = 1.0 - p

        full_kelly = (b * p - q) / b
        recommended_kelly = full_kelly * self.max_kelly_fraction
        recommended_stake = bankroll * recommended_kelly

        return {
            'implied_prob': round(implied_prob * 100, 1),
            'actual_prob': round(actual_prob * 100, 1),
            'edge': round((actual_prob - implied_prob) * 100, 1),
            'full_kelly_percent': round(full_kelly * 100, 2),
            'recommended_kelly_percent': round(recommended_kelly * 100, 2),
            'recommended_stake': round(recommended_stake, 2)
        }


# =============================================================================
# FIXED ARBITRAGE SYSTEM
# =============================================================================

class FixedArbitrageSystem:
    def __init__(self):
        self.duplicate_detector = EnhancedDuplicateDetector()
        self.bankroll_manager = KellyBankrollManager()
        self.odds_parser = CrazyNinjaOddsParser()

    def process_opportunities(self, opportunities: List[Dict], current_bankroll: float, pending_bets: list) -> List[
        Dict]:
        print("Processing opportunities...")
        print(f"Received {len(opportunities)} opportunities")

        filtered_opportunities = []

        for i, opp in enumerate(opportunities):
            odds_str = opp.get('odds', '')
            sportsbook = opp.get('sportsbook', 'Unknown')

            # Use the specialized CrazyNinja odds parser
            decimal_odds = self.odds_parser.parse_crazyninja_odds(odds_str)
            if decimal_odds is None:
                print(f"Could not convert odds: '{odds_str}'")
                continue

            ev_value = float(opp.get('ev', 0))
            event = opp.get('event', '')
            market = opp.get('market', '')
            player = opp.get('player', 'Unknown')

            # Less strict duplicate detection
            if self.duplicate_detector.is_duplicate_bet(event, market, decimal_odds):
                print(f"Skipping duplicate: {event} - {player}")
                continue

            # Kelly-based stake calculation
            stake = self.bankroll_manager.calculate_stake(ev_value, current_bankroll, pending_bets, decimal_odds)
            if stake <= 0:
                kelly_info = self.bankroll_manager.get_kelly_calculation_details(ev_value, decimal_odds,
                                                                                 current_bankroll)
                if kelly_info:
                    print(
                        f"No bet: {event} - {player} (Edge: {kelly_info['edge']}%, Kelly: {kelly_info['recommended_kelly_percent']}%, Book: {sportsbook})")
                else:
                    print(
                        f"No bet: {event} - {player} (EV: {ev_value:.1%}, Odds: {decimal_odds:.2f}, Book: {sportsbook})")
                continue

            filtered_opp = opp.copy()
            filtered_opp['decimal_odds'] = decimal_odds
            filtered_opp['calculated_stake'] = stake
            filtered_opp['sportsbook'] = sportsbook
            filtered_opportunities.append(filtered_opp)

            self.duplicate_detector.add_bet(event, market, decimal_odds)

            # Show Kelly calculation details
            kelly_info = self.bankroll_manager.get_kelly_calculation_details(ev_value, decimal_odds, current_bankroll)
            stake_percent = (stake / current_bankroll * 100) if current_bankroll > 0 else 0

            print(f"APPROVED: {event}")
            print(
                f"   Player: {player} | Market: {market} | Odds: {decimal_odds:.2f} | EV: {ev_value:.1%} | Book: {sportsbook}")
            print(f"   Kelly: {kelly_info['recommended_kelly_percent']}% | Stake: {stake_percent:.2f}% (€{stake:.2f})")
            print(f"   Edge: +{kelly_info['edge']}% | Win Prob: {kelly_info['actual_prob']}%")

        print(f"Filtered to {len(filtered_opportunities)} bettable opportunities")
        return filtered_opportunities


# =============================================================================
# ENHANCED PAPER TRADING SYSTEM WITH ALL COMPONENTS
# =============================================================================

class EnhancedPaperTradingSystem:
    def __init__(self):
        # Use configuration values
        self.bankroll = config.bankroll.initial_bankroll

        # Choose database based on configuration
        self.db = create_database_manager()
        db_type = "Supabase" if config.database.use_supabase else "SQLite"
        print(f"Using {db_type} database")

        # Initialize components
        self.scanner = UltraStableScanner()
        self.arbitrage_system = FixedArbitrageSystem()
        self.game_matcher = EnhancedGameMatcher()

        # Add Real Stats Resolver
        self.real_stats_resolver = RStatsResolver()

        # Initialize Idempotent Resolution Manager
        self.idempotent_resolver = IdempotentResolutionManager(self.db, self.real_stats_resolver)

        # Load existing state from database
        self._load_bankroll_state()
        self.setup_directories()

    def setup_directories(self):
        os.makedirs('data', exist_ok=True)

    def _load_bankroll_state(self):
        """Load bankroll state from database"""
        try:
            stats = self.db.get_performance_stats()
            total_profit = stats.get('total_profit', 0)
            if stats['total_bets'] > 0 and total_profit is not None:
                self.bankroll = config.bankroll.initial_bankroll + total_profit
                print(f"Loaded bankroll state: €{self.bankroll:.2f}")
            else:
                print(f"Starting with initial bankroll: €{self.bankroll:.2f}")
        except Exception as e:
            print(f"Could not load bankroll state: {e}")
            print(f"Starting with initial bankroll: €{self.bankroll:.2f}")

    def get_pending_bets(self):
        return self.db.get_pending_bets()

    def _extract_game_date_from_event(self, event: str) -> Optional[str]:
        """Extract game date from event string if available"""
        try:
            # Look for date patterns in the event
            patterns = [
                r'(\d{1,2}/\d{1,2}/\d{4})',  # MM/DD/YYYY
                r'(\d{1,2}/\d{1,2})',  # MM/DD (assume current year)
                r'(\d{4}-\d{2}-\d{2})',  # YYYY-MM-DD
                r'(\w+ \d{1,2}, \d{4})'  # Month Day, Year
            ]

            for pattern in patterns:
                match = re.search(pattern, event)
                if match:
                    date_str = match.group(1)

                    # Parse based on format
                    if '/' in date_str:
                        parts = date_str.split('/')
                        if len(parts) == 3:  # MM/DD/YYYY
                            month, day, year = parts
                            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                        elif len(parts) == 2:  # MM/DD (assume current year)
                            month, day = parts
                            year = datetime.now().year
                            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"

                    elif '-' in date_str and len(date_str) == 10:  # YYYY-MM-DD
                        return date_str

            # If no date found, use today
            today = datetime.now().strftime('%Y-%m-%d')
            return today

        except Exception as e:
            print(f"Could not extract date from event '{event}': {e}")
            return None

    def place_enhanced_bet(self, opportunity):
        """Place bet with enhanced tracking and database storage"""
        try:
            pending_bets = self.db.get_pending_bets()
            sportsbook = opportunity.get('sportsbook', 'Unknown')

            # Parse odds
            odds_decimal = self.arbitrage_system.odds_parser.parse_crazyninja_odds(opportunity.get('odds', '1.0'))
            if odds_decimal is None:
                print(f"Could not parse odds: {opportunity.get('odds')}")
                return False

            # Kelly-based stake calculation
            stake = self.arbitrage_system.bankroll_manager.calculate_stake(
                opportunity['ev'], self.bankroll, pending_bets, odds_decimal
            )

            if stake <= 0:
                print(f"Stake too low: {stake} (Book: {sportsbook})")
                return False

            sport = opportunity.get('sport', 'unknown')
            if sport.lower() == 'unknown':
                sport = self.detect_sport_from_event(opportunity['event'])

            # Get Kelly details for tracking
            kelly_info = self.arbitrage_system.bankroll_manager.get_kelly_calculation_details(
                opportunity['ev'], odds_decimal, self.bankroll
            )

            # ========== FIX: Prioritize scanner's game_date ==========
            # The scanner already extracted the game_date, use it!
            game_date = opportunity.get('game_date')

            # If not found, try to extract from event
            if not game_date:
                game_date = self._extract_game_date_from_event(opportunity['event'])

            # Debug: Show what date we found
            print(f"DEBUG: Using game_date: {game_date} (from opportunity: {opportunity.get('game_date')})")
            # ========== END FIX ==========

            bet_data = {
                'event': opportunity['event'],
                'sport': sport,
                'market': opportunity.get('market', ''),
                'player': opportunity.get('player', 'Unknown'),
                'odds': odds_decimal,
                'stake': stake,
                'potential_win': stake * odds_decimal,
                'ev': opportunity['ev'],
                'sportsbook': sportsbook,
                'kelly_percent': kelly_info.get('recommended_kelly_percent'),
                'edge_percent': kelly_info.get('edge'),
                'game_date': game_date  # Make sure this is included
            }

            # Save to database
            if hasattr(self.db, 'save_bet'):
                bet_id = self.db.save_bet(bet_data)
            else:
                print("Database doesn't support save_bet method")
                return False

            # Update bankroll
            self.bankroll -= stake

            stake_percent = (stake / (self.bankroll + stake) * 100) if (self.bankroll + stake) > 0 else 0

            db_type = "Supabase" if config.database.use_supabase else "SQLite"
            player_name = opportunity.get('player', 'Unknown Player')
            print(f"ENHANCED BET PLACED ({db_type}): {opportunity['sport']}")
            print(f"   Player: {player_name} | Market: {opportunity['market']}")
            print(f"   Event: {opportunity['event']} | Odds: {odds_decimal:.2f} | EV: {opportunity['ev']:.1%}")
            print(f"   Book: {sportsbook} | Stake: {stake_percent:.2f}% (€{stake:.2f}) | Edge: +{kelly_info['edge']}%")
            if game_date:
                print(f"   Game Date: {game_date}")
            print(f"   Database ID: {bet_id}")

            return True

        except Exception as e:
            print(f"Error placing enhanced bet: {e}")
            return False

    def detect_sport_from_event(self, event):
        """Simple sport detection"""
        event_lower = event.lower()
        if any(team in event_lower for team in ['lakers', 'warriors', 'celtics', 'heat', 'nba']):
            return 'nba'
        elif any(team in event_lower for team in ['bengals', 'ravens', 'packers', 'lions', 'nfl']):
            return 'nfl'
        return 'sports'

    def automated_scanning(self, scan_interval=300, max_scans=2):
        """Enhanced automated scanning"""
        print(f"STARTING ENHANCED AUTOMATED SCANNING")
        db_type = "Supabase" if config.database.use_supabase else "SQLite"
        print(f"Will run {max_scans} scans using {db_type}")
        print("=" * 50)

        for scan_num in range(1, max_scans + 1):
            print(f"\nSCAN #{scan_num}/{max_scans} | Bankroll: €{self.bankroll:.2f}")
            print("-" * 50)

            # Scan for opportunities
            opportunities = self.scanner.scrape_crazyninja_odds()

            # Get current pending bets from database
            pending_bets = self.db.get_pending_bets()

            # Process opportunities
            filtered_opps = self.arbitrage_system.process_opportunities(opportunities, self.bankroll, pending_bets)

            # Place bets using enhanced system
            bets_placed = 0
            for opp in filtered_opps[:config.betting.max_bets_per_scan]:
                if self.place_enhanced_bet(opp):
                    bets_placed += 1

            # Try to resolve using idempotent resolver
            if pending_bets:
                print(f"\nChecking for completed games with idempotent resolver...")
                batch_result = self.idempotent_resolver.batch_resolve(max_bets=len(pending_bets))

                if batch_result['complete'] > 0:
                    print(f"   Idempotent resolver completed {batch_result['complete']} bets")
                    # Update bankroll from database
                    stats = self.db.get_performance_stats()
                    self.bankroll = config.bankroll.initial_bankroll + stats.get('total_profit', 0)
                else:
                    print(f"   No bets resolved (may be pending completion)")

            # Show status
            pending = self.db.get_pending_bets()
            print(f"\nStatus: {len(pending)} pending bets, €{self.bankroll:.2f} bankroll")

            if scan_num < max_scans:
                print(f"Waiting {scan_interval} seconds before next scan...")
                time.sleep(min(scan_interval, 60))

        print("Automated scanning completed")

    def view_enhanced_performance_dashboard(self):
        """Enhanced performance dashboard with database metrics"""
        stats = self.db.get_performance_stats()

        if stats['total_bets'] == 0:
            print("No bets recorded yet")
            return

        pending_bets = self.db.get_pending_bets()
        db_type = "Supabase" if config.database.use_supabase else "SQLite"

        print(f"\nENHANCED PERFORMANCE DASHBOARD ({db_type})")
        print("=" * 50)
        print(f"Current Bankroll: €{self.bankroll:.2f}")
        print(f"Total Profit/Loss: €{stats['total_profit']:.2f}")
        print(f"Total Wagered: €{stats['total_wagered']:.2f}")
        print(f"Total Bets: {stats['total_bets']}")
        print(f"Won: {stats['won_bets']} | Lost: {stats['lost_bets']} | Pending: {stats['pending_bets']}")
        print(f"Win Rate: {stats['win_rate']:.1f}%")

        # Calculate ROI
        if stats['total_wagered'] > 0:
            roi = (stats['total_profit'] / stats['total_wagered']) * 100
            print(f"ROI: {roi:.1f}%")

    def view_enhanced_recent_bets(self, limit=10):
        """View recent bets from database"""
        recent_bets = self.db.get_bet_history(limit)
        db_type = "Supabase" if config.database.use_supabase else "SQLite"

        if not recent_bets:
            print("No bets recorded yet")
            return

        print(f"\nLAST {len(recent_bets)} BETS (FROM {db_type.upper()})")
        print("=" * 90)

        for bet in recent_bets:
            status_icon = 'WON' if bet['status'] == 'won' else 'LOST' if bet['status'] == 'lost' else 'PENDING'
            player_name = bet.get('player', 'Unknown Player')
            print(f"{status_icon} Bet #{bet['id']}: {bet['event']} - {player_name}")
            print(f"   Sport: {bet['sport']} | Market: {bet['market']}")
            print(f"   Odds: {bet['odds']:.2f} | Stake: €{bet['stake']:.2f} | EV: {bet['ev']:.1%}")
            print(f"   Book: {bet['sportsbook']} | Kelly: {bet.get('kelly_percent', 0):.2f}%")
            print(f"   Game Date: {bet.get('game_date', 'Unknown')}")
            if bet['status'] != 'pending':
                print(f"   Profit: €{bet['profit']:.2f} | Result: {bet['result']}")

            # Handle different date formats
            placed_at = bet.get('placed_at', 'Unknown')
            if isinstance(placed_at, str) and 'T' in placed_at:
                placed_at = placed_at.replace('T', ' ').split('.')[0]
            print(f"   Placed: {placed_at}")

            if bet.get('resolved_at'):
                resolved_at = bet['resolved_at']
                if isinstance(resolved_at, str) and 'T' in resolved_at:
                    resolved_at = resolved_at.replace('T', ' ').split('.')[0]
                print(f"   Resolved: {resolved_at}")
            print()

    def manually_resolve_bet_enhanced(self):
        """Manual bet resolution with database"""
        pending_bets = self.db.get_pending_bets()
        if not pending_bets:
            print("No pending bets to resolve")
            return

        print("\nPENDING BETS:")
        for bet in pending_bets:
            player_name = bet.get('player', 'Unknown Player')
            game_date = bet.get('game_date', 'Unknown')
            print(f"   #{bet['id']}: {bet['event']} - {player_name} | Sport: {bet['sport']} | Date: {game_date}")

        try:
            bet_id_input = input("\nEnter Bet ID to resolve: ").strip()

            # Handle different ID types
            try:
                if '.' in bet_id_input or bet_id_input.isdigit():
                    bet_id = int(bet_id_input)  # SQLite integer ID
                else:
                    bet_id = bet_id_input  # Supabase string UUID
            except ValueError:
                bet_id = bet_id_input  # Use as string if conversion fails

            result = input("Result (win/loss): ").strip().lower()

            bet = next((b for b in pending_bets if str(b['id']) == str(bet_id)), None)
            if not bet:
                print("Bet ID not found")
                return

            if result == 'win':
                profit = bet['stake'] * (bet['odds'] - 1)
                if hasattr(self.db, 'update_bet_result'):
                    self.db.update_bet_result(bet_id, True, profit)
                self.bankroll += bet['stake'] + profit
                player_name = bet.get('player', 'Unknown Player')
                print(f"Manually resolved bet #{bet_id} - {player_name} as WIN")
                print(f"   Profit: €{profit:.2f} | New Bankroll: €{self.bankroll:.2f}")
            elif result == 'loss':
                if hasattr(self.db, 'update_bet_result'):
                    self.db.update_bet_result(bet_id, False, -bet['stake'])
                player_name = bet.get('player', 'Unknown Player')
                print(f"Manually resolved bet #{bet_id} - {player_name} as LOSS")
            else:
                print("Invalid result. Use 'win' or 'loss'")

        except Exception as e:
            print(f"Error: {e}")

    def idempotent_resolution_menu(self):
        """Menu for idempotent resolution manager"""
        while True:
            print("\n" + "=" * 50)
            print("IDEMPOTENT RESOLUTION MANAGER")
            print("=" * 50)
            print("1. Resolve Single Bet Safely")
            print("2. Batch Resolve Pending Bets")
            print("3. View Resolution Audit Trail")
            print("4. Clean Up Old Attempts")
            print("5. View R Resolver Performance")
            print("0. Back to Main Menu")

            choice = input("\nSelect option: ").strip()

            if choice == '1':
                self._idempotent_resolve_single()
            elif choice == '2':
                self._idempotent_batch_resolve()
            elif choice == '3':
                self._view_audit_trail()
            elif choice == '4':
                self._cleanup_old_attempts()
            elif choice == '5':
                self._view_r_resolver_stats()
            elif choice == '0':
                break
            else:
                print("Invalid option")

    def _idempotent_resolve_single(self):
        """Resolve a single bet with idempotent manager"""
        pending_bets = self.db.get_pending_bets()
        if not pending_bets:
            print("No pending bets to resolve")
            return

        print("\nPENDING BETS:")
        for i, bet in enumerate(pending_bets):
            player_name = bet.get('player', 'Unknown Player')
            print(f"   {i + 1}. #{bet['id']}: {player_name} - {bet.get('market', '')}")

        try:
            selection = input("\nSelect bet number (or Bet ID): ").strip()

            if selection.isdigit() and int(selection) <= len(pending_bets):
                bet = pending_bets[int(selection) - 1]
                bet_id = bet['id']
            else:
                bet_id = selection

            force = input("Force resolution (y/N)? ").strip().lower() == 'y'

            print(f"\nResolving bet {bet_id}...")
            result = self.idempotent_resolver.resolve_bet_safe(bet_id, force)

            print(f"\nRESULT:")
            print(f"   State: {result['state']}")
            if result['state'] == 'complete':
                print(f"   Result: {result['result']}")
                print(f"   Profit: €{result['profit']:.2f}")
            elif result['state'] == 'retry_pending':
                print(f"   Error: {result.get('error', 'No error')}")
                print(f"   Next retry: {result.get('next_retry')}")
            elif result['state'] == 'failed_permanent':
                print(f"   Error: {result.get('error', 'No error')}")

            print(f"   Resolution ID: {result['resolution_id']}")

        except Exception as e:
            print(f"Error: {e}")

    def _idempotent_batch_resolve(self):
        """Batch resolve with idempotent manager"""
        try:
            max_bets = input("Max bets to resolve (default 10): ").strip()
            max_bets = int(max_bets) if max_bets.isdigit() else 10

            force = input("Force resolution (y/N)? ").strip().lower() == 'y'

            print(f"\nBatch resolving up to {max_bets} bets...")
            result = self.idempotent_resolver.batch_resolve(max_bets, force)

            print(f"\nBATCH RESULT:")
            print(f"   Batch ID: {result['batch_id']}")
            print(f"   Total processed: {result['total']}")
            print(f"   Complete: {result['complete']}")
            print(f"   Failed permanently: {result['failed_permanent']}")
            print(f"   Retry pending: {result['retry_pending']}")
            print(f"   Manager errors: {result['manager_errors']}")

            # Update bankroll
            if result['complete'] > 0:
                pending_bets = self.db.get_pending_bets()
                stats = self.db.get_performance_stats()
                self.bankroll = config.bankroll.initial_bankroll + stats.get('total_profit', 0)
                print(f"   Updated bankroll: €{self.bankroll:.2f}")

        except Exception as e:
            print(f"Error: {e}")

    def _view_audit_trail(self):
        """View resolution audit trail"""
        bet_id = input("Enter Bet ID (or leave blank for all): ").strip()

        audit_trail = self.idempotent_resolver.get_audit_trail(bet_id if bet_id else None)

        if not audit_trail:
            print("No audit entries found")
            return

        print(f"\nAUDIT TRAIL ({'All bets' if not bet_id else 'Bet ' + bet_id})")
        print("=" * 80)

        for entry in audit_trail[-20:]:  # Show last 20 entries
            timestamp = entry['timestamp'].replace('T', ' ').split('.')[0]
            print(f"{timestamp} | {entry['event'].upper():15} | {entry['message']}")

    def _cleanup_old_attempts(self):
        """Clean up old resolution attempts"""
        try:
            hours = input("Clean up attempts older than (hours, default 168): ").strip()
            hours = int(hours) if hours.isdigit() else 168

            self.real_stats_resolver.cleanup_old_attempts(hours)
            print(f"Cleaned up attempts older than {hours} hours")

        except Exception as e:
            print(f"Error: {e}")

    def _view_r_resolver_stats(self):
        """View R resolver performance statistics"""
        stats = self.real_stats_resolver.get_performance_stats()

        print("\nR RESOLVER PERFORMANCE")
        print("=" * 50)
        print(f"R Calls Total: {stats['r_calls_total']}")
        print(f"R Calls Successful: {stats['r_successful']}")
        print(f"R Success Rate: {stats['r_success_rate']:.1f}%")
        print(f"Active Bets: {stats['active_bets']}")
        print(f"Active Resolution Attempts: {stats['active_resolution_attempts']}")
        print(f"Cache Size: {stats['cache_size']} entries")
        print(f"Retry Cooldown: {stats['retry_cooldown_hours']} hours")
        print(f"Max Attempts per Bet: {stats['max_attempts_per_bet']}")

    def debug_test_r_script(self):
        """Debug: Test R script directly with a known example"""
        print("\n" + "=" * 50)
        print("DEBUG: TESTING R SCRIPT DIRECTLY")
        print("=" * 50)

        # Test with a known working case
        test_cases = [
            {
                "name": "Patrick Mahomes passing yards",
                "player_name": "Patrick Mahomes",
                "sport": "nfl",
                "season": 2023,
                "market_type": "player passing yards over",
                "event_string": "Kansas City Chiefs @ Miami Dolphins",
                "line_value": 275.5,
                "game_date": "2024-01-13"
            },
            {
                "name": "LeBron James points",
                "player_name": "LeBron James",
                "sport": "nba",
                "season": 2024,
                "market_type": "player points over",
                "event_string": "Los Angeles Lakers @ Golden State Warriors",
                "line_value": 25.5,
                "game_date": "2024-12-14"
            }
        ]

        print("Available test cases:")
        for i, case in enumerate(test_cases):
            print(f"  {i + 1}. {case['name']}")

        try:
            selection = input("\nSelect test case (1-2): ").strip()
            if selection not in ["1", "2"]:
                print("Invalid selection")
                return

            test_case = test_cases[int(selection) - 1]

            print(f"\nTesting: {test_case['name']}")
            print(f"  Player: {test_case['player_name']}")
            print(f"  Sport: {test_case['sport']}")
            print(f"  Season: {test_case['season']}")
            print(f"  Market: {test_case['market_type']}")
            print(f"  Event: {test_case['event_string']}")
            print(f"  Line: {test_case['line_value']}")
            print(f"  Game Date: {test_case['game_date']}")

            # Call R directly
            r_result = self.real_stats_resolver._call_r_script(
                player_name=test_case['player_name'],
                sport=test_case['sport'],
                season=test_case['season'],
                market_type=test_case['market_type'],
                event_string=test_case['event_string'],
                line_value=test_case['line_value'],
                game_date=test_case['game_date']
            )

            print(f"\nR SCRIPT RESULT:")
            print(json.dumps(r_result, indent=2))

            if r_result.get('success'):
                if r_result.get('resolved'):
                    print(f"\n✓ Bet successfully resolved!")
                    if 'bet_won' in r_result:
                        print(f"   Result: {'WIN' if r_result['bet_won'] else 'LOSS'}")
                    if 'actual_value' in r_result:
                        print(f"   Actual value: {r_result['actual_value']}")
                else:
                    print(f"\n⚠ Bet not resolved yet")
                    print(f"   Reason: {r_result.get('error', 'No error message')}")
            else:
                print(f"\n✗ R script failed")
                print(f"   Error: {r_result.get('error', 'Unknown error')}")

        except Exception as e:
            print(f"Error during debug test: {e}")
            import traceback
            traceback.print_exc()

    def debug_r_resolver_directly(self):
        """Debug R resolver directly"""
        print("\n" + "=" * 80)
        print("DEBUG: TEST R RESOLVER DIRECTLY")
        print("=" * 80)

        # Get a pending bet to test
        pending_bets = self.db.get_pending_bets()
        if not pending_bets:
            print("No pending bets to test with")
            return

        # Take the first pending bet
        bet = pending_bets[0]
        print(f"Testing with bet: {bet.get('player', 'Unknown')} - {bet.get('market', 'Unknown')}")
        print(f"Event: {bet['event']}")
        print(f"Game Date from DB: {bet.get('game_date', 'Not found')}")

        # Prepare bet data for resolver
        test_bet = {
            'player': bet.get('player', ''),
            'player_name': bet.get('player', ''),  # Add both keys
            'event': bet['event'],
            'sport': bet.get('sport', '').lower(),
            'market': bet.get('market', ''),
            'stake': float(bet.get('stake', 0)),
            'odds': float(bet.get('odds', 0)),
            'game_date': bet.get('game_date')  # Make sure this is included
        }

        print(f"\nBet data for R resolver:")
        print(f"  Player: {test_bet['player']}")
        print(f"  Sport: {test_bet['sport']}")
        print(f"  Market: {test_bet['market']}")
        print(f"  Game Date: {test_bet.get('game_date', 'Not provided')}")

        try:
            # Test the _should_resolve_bet method
            should_resolve = self.real_stats_resolver._should_resolve_bet(test_bet)
            print(f"\nShould resolve? {should_resolve}")

            if should_resolve:
                print("\nAttempting to resolve with R resolver...")

                # Extract season hint
                season = self.real_stats_resolver._extract_season_hint(
                    test_bet['event'],
                    test_bet['sport'],
                    test_bet.get('game_date')
                )
                print(f"Extracted season: {season}")

                # Try to resolve
                result = self.real_stats_resolver._call_r_script(
                    player_name=test_bet['player'],
                    sport=test_bet['sport'],
                    season=season,
                    market_type='player_prop',  # Default
                    event_string=test_bet['event'],
                    line_value=None,
                    game_date=test_bet.get('game_date')
                )

                print(f"\nR SCRIPT RAW RESULT:")
                print(json.dumps(result, indent=2))

                if result.get('success'):
                    if result.get('resolved'):
                        print(f"\n✓ R script reports bet is resolved!")
                        print(f"  Data: {result.get('data', {})}")
                    else:
                        print(f"\n⚠ R script reports bet not resolved")
                        print(f"  Reason: {result.get('reason', 'No reason given')}")
                else:
                    print(f"\n✗ R script failed")
                    print(f"  Error: {result.get('error', 'Unknown error')}")

        except Exception as e:
            print(f"\nError during R resolver test: {e}")
            import traceback
            traceback.print_exc()

    def test_sport_normalization(self):
        """Test if sport normalization fixes the issue"""
        print("\n" + "=" * 80)
        print("TEST SPORT NORMALIZATION FIX")
        print("=" * 80)

        # Test with "football" which should normalize to "nfl"
        test_data = {
            'player_name': 'Jeremy Ruckert',
            'sport': 'football',  # This should now be normalized to "nfl"
            'season': 2025,
            'market_type': 'last_scorer',
            'event_string': 'Miami Dolphins @ New York Jets',
            'line_value': None,
            'game_date': '2025-12-07'
        }

        print("Testing with sport='football' (should normalize to 'nfl'):")
        for key, value in test_data.items():
            print(f"  {key}: {value}")

        try:
            r_result = self.real_stats_resolver._call_r_script(**test_data)

            print(f"\nR SCRIPT RESULT:")
            print(json.dumps(r_result, indent=2))

            if r_result.get('success'):
                if r_result.get('resolved'):
                    print(f"\n✅ SUCCESS! Sport normalization worked!")
                    print(f"   Bet resolved successfully")
                else:
                    print(f"\n⚠ Bet not resolved but game found")
                    if 'game_id' in r_result:
                        print(f"   Game ID: {r_result['game_id']}")
            else:
                print(f"\n❌ Still failing")
                print(f"   Error: {r_result.get('error', 'Unknown')}")

        except Exception as e:
            print(f"Error: {e}")
    def debug_check_bet_dates(self):
        """Debug: Check if bets have game_date in database"""
        print("\n" + "=" * 80)
        print("DEBUG: CHECKING BET DATES IN DATABASE")
        print("=" * 80)

        # Get a few recent bets from Supabase
        try:
            # Get recent bets directly
            response = self.db.supabase.table('bets').select('*').order('placed_at', desc=True).limit(5).execute()
            recent_bets = response.data if response.data else []

            if not recent_bets:
                print("No bets found in database")
                return

            print(f"Found {len(recent_bets)} recent bets")

            for i, bet in enumerate(recent_bets):
                print(f"\nBet #{i + 1}:")
                print(f"  ID: {bet.get('id')}")
                print(f"  Player: {bet.get('player', 'Unknown')}")
                print(f"  Event: {bet.get('event', 'Unknown')}")
                print(f"  Game Date field: {bet.get('game_date', 'MISSING!')}")
                print(f"  All fields with values:")
                for key, value in bet.items():
                    if value is not None and value != '':
                        print(f"    - {key}: {value}")

        except Exception as e:
            print(f"Error checking database: {e}")

    def run(self):
        """Enhanced main menu with all options"""
        while True:
            pending_count = len(self.db.get_pending_bets())
            db_type = "Supabase" if config.database.use_supabase else "SQLite"

            print("\n" + "=" * 60)
            print(f"ENHANCED PAPER TRADING SYSTEM ({db_type.upper()})")
            print("=" * 60)
            print(f"Current Bankroll: €{self.bankroll:.2f}")
            print(f"Pending Bets: {pending_count}")
            print(
                f"Config: {config.betting.min_ev_threshold:.1%} min EV, {config.betting.max_bets_per_scan} max bets/scan")
            print()
            print("1. Continuous Automated Scanning (2 scans max)")
            print("2. Single Manual Scan & Resolution")
            print("3. View Enhanced Performance Dashboard")
            print("4. View Enhanced Recent Bets")
            print("5. Export Enhanced Data File")
            print("6. Idempotent Resolution Manager")
            print("7. Manual +EV Scan Only")
            print("8. Manually Resolve Specific Bet")
            print("9. Show Current Configuration")
            print("10. Test RealStatsResolver (DEBUG)")
            print("11. DEBUG: Test R Script Directly")
            print("12. DEBUG: Test R Team Matching")  # ADDED
            print("13. DEBUG: Check Pending Bet Dates")  # ADDED
            print("14. DEBUG: Check R Packages")  # ADDED
            print("15. DEBUG: NFL Schedule Lookup")  # ADDED
            print("16. DEBUG: Deep Schedule Diagnostic")  # ADDED
            print("17. DEBUG: Test Exact Pending Bet")  # ADD
            print("18. DEBUG: Debug R Script Directly")  # ADD
            print("19. TEST: Sport Normalization Fix")  # <-- ADD THIS LINE
            print("0. Exit")
            print()

            choice = input("Select option: ").strip()

            if choice == '1':
                interval = input("Scan interval (seconds, default 300): ").strip()
                interval = int(interval) if interval.isdigit() else config.scanner.scan_interval
                self.automated_scanning(scan_interval=interval, max_scans=2)

            elif choice == '2':
                print("\nSINGLE MANUAL SCAN")
                opportunities = self.scanner.scrape_crazyninja_odds()
                for opp in opportunities[:config.betting.max_bets_per_scan]:
                    self.place_enhanced_bet(opp)
                # Use idempotent resolver for resolution
                if self.db.get_pending_bets():
                    print(f"\nAttempting resolution with idempotent resolver...")
                    result = self.idempotent_resolver.batch_resolve(max_bets=len(self.db.get_pending_bets()))
                    print(f"   Result: {result['complete']} completed, {result['retry_pending']} pending retry")

            elif choice == '3':
                self.view_enhanced_performance_dashboard()

            elif choice == '4':
                limit = input("Number of bets to show (default 10): ").strip()
                limit = int(limit) if limit.isdigit() else 10
                self.view_enhanced_recent_bets(limit)

            elif choice == '5':
                self.export_enhanced_data()

            elif choice == '6':
                self.idempotent_resolution_menu()

            elif choice == '7':
                opportunities = self.scanner.scrape_crazyninja_odds()
                if opportunities:
                    print(f"Found {len(opportunities)} +EV opportunities")
                else:
                    print("No +EV opportunities found")

            elif choice == '8':
                self.manually_resolve_bet_enhanced()

            elif choice == '9':
                self.show_configuration()

            elif choice == '10':
                self.test_realstats_resolver()

            elif choice == '11':
                self.debug_test_r_script()

            elif choice == '12':  # ADDED: Test R Team Matching
                self.test_r_team_matching()

            elif choice == '13':  # ADDED: Check Pending Bet Dates
                self.check_pending_bet_dates()

            elif choice == '14':  # ADDED: Check R Packages
                self.check_r_packages()

            elif choice == '15':  # ADDED: NFL Schedule Lookup
                self.debug_nfl_schedule_lookup()

            elif choice == '16':  # ADDED: Deep Schedule Diagnostic
                self.debug_schedule_load_issue()

            elif choice == '17':
                self.test_exact_pending_bet()

            elif choice == '18':
                self.debug_r_script_directly()
            elif choice == '19':
                self.test_sport_normalization()
            elif choice == '0':
                print("Exiting enhanced system...")
                if hasattr(self, 'scanner'):
                    self.scanner.close()
                break

            else:
                print("Invalid option")

    def show_configuration(self):
        """Show current configuration"""
        db_type = "Supabase" if config.database.use_supabase else "SQLite"
        print(f"\nCURRENT CONFIGURATION ({db_type.upper()})")
        print("=" * 40)
        print(f"Bankroll: €{config.bankroll.initial_bankroll:.2f}")
        print(f"Min EV: {config.betting.min_ev_threshold:.1%}")
        print(f"Max Bets/Scan: {config.betting.max_bets_per_scan}")
        print(f"Scan Interval: {config.scanner.scan_interval}s")
        print(f"Kelly Fraction: {config.bankroll.max_kelly_fraction:.0%}")
        print(f"Stake Range: {config.bankroll.min_stake_percent:.1%}-{config.bankroll.max_stake_percent:.1%}")
        print(f"Database: {db_type}")
        if config.database.use_supabase:
            print(f"Supabase URL: {config.database.supabase_url}")
        else:
            print(f"SQLite Path: {config.database.db_path}")
        print(f"Logging: {config.logging.level} -> {config.logging.file}")

    def test_realstats_resolver(self):
        """Test the RealStatsResolver with a specific bet"""
        pending_bets = self.db.get_pending_bets()
        if not pending_bets:
            print("No pending bets to test with")
            return

        print("\nTESTING REALSTATS RESOLVER")
        print("=" * 40)

        for i, bet in enumerate(pending_bets[:3]):  # Test with first 3 bets
            try:
                player_name = bet.get('player', 'Unknown Player')
                print(f"\nTest #{i + 1}: {player_name} - {bet.get('market', 'Unknown')}")
                print(f"   Event: {bet['event']}")

                # Prepare bet data for RealStatsResolver
                test_bet = {
                    'player': bet.get('player', ''),
                    'event': bet.get('event', ''),
                    'sport': bet.get('sport', ''),
                    'market': bet.get('market', ''),
                    'stake': float(bet.get('stake', 0)),
                    'odds': float(bet.get('odds', 0))
                }

                # Try to resolve
                print(f"   Calling RealStatsResolver...")
                won, profit = self.real_stats_resolver.resolve_player_prop(test_bet)

                print(f"   REAL STATS RESULT: {'WIN' if won else 'LOSS'} (€{profit:+.2f})")

            except Exception as e:
                error_msg = str(e)
                print(f"   RealStatsResolver error: {error_msg}")

                if "Game likely not completed yet" in error_msg:
                    print(f"   Game hasn't completed yet")
                elif "Real stats not found" in error_msg:
                    print(f"   Couldn't find real stats")
                else:
                    print(f"   Unknown error")

    def export_enhanced_data(self):
        """Export enhanced data for upload"""
        bets = self.db.get_bet_history(1000)

        if not bets:
            print("No data to export")
            return

        export_data = []
        for bet in bets:
            # Handle different date formats
            placed_at = bet.get('placed_at', 'Unknown')
            if isinstance(placed_at, str) and 'T' in placed_at:
                placed_at = placed_at.replace('T', ' ').split('.')[0]

            resolved_at = bet.get('resolved_at', 'N/A')
            if resolved_at and isinstance(resolved_at, str) and 'T' in resolved_at:
                resolved_at = resolved_at.replace('T', ' ').split('.')[0]

            export_data.append({
                'ID': bet['id'],
                'Player': bet.get('player', 'Unknown'),
                'Event': bet['event'],
                'Sport': bet['sport'],
                'Market': bet['market'],
                'Odds': bet['odds'],
                'Stake': bet['stake'],
                'EV': f"{bet['ev']:.1%}",
                'Sportsbook': bet['sportsbook'],
                'Kelly_Percent': f"{bet.get('kelly_percent', 0):.2f}%",
                'Edge_Percent': f"{bet.get('edge_percent', 0):.1f}%",
                'Status': bet['status'],
                'Profit': bet['profit'],
                'Game_Date': bet.get('game_date', 'Unknown'),
                'Placed_At': placed_at,
                'Resolved_At': resolved_at
            })

        df = pd.DataFrame(export_data)
        filename = f"enhanced_trading_export_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        df.to_csv(filename, index=False)
        print(f"Exported {len(export_data)} enhanced bets to {filename}")

    # Add these methods to your EnhancedPaperTradingSystem class:

    def test_r_team_matching(self):
        """Test R script team matching directly"""
        print("\n" + "=" * 80)
        print("TEST R TEAM MATCHING")
        print("=" * 80)

        # Simple test case
        test_data = {
            'player_name': 'Test Player',
            'sport': 'nfl',
            'season': 2023,
            'market_type': 'player_prop',
            'event_string': 'Kansas City Chiefs @ Miami Dolphins',
            'line_value': None,
            'game_date': '2023-11-05'  # Real game date
        }

        print(f"Test data: {json.dumps(test_data, indent=2)}")

        try:
            # First, let's see what the R script does with just team matching
            print("\nTesting R script directly...")
            r_result = self.real_stats_resolver._call_r_script(**test_data)

            print(f"\nR SCRIPT RESULT:")
            print(json.dumps(r_result, indent=2))

            # Check if we can see debug output
            if 'error' in r_result and 'DEBUG' not in r_result.get('error', ''):
                print("\nChecking R script output for debug info...")

                # Let's manually run the R command to see debug output
                cmd = [
                    self.real_stats_resolver.rscript_path,
                    self.real_stats_resolver.r_script_path,
                    test_data['player_name'],
                    test_data['sport'],
                    str(test_data['season']),
                    test_data['market_type'],
                    test_data['event_string'],
                    "NULL",
                    test_data['game_date']
                ]

                print(f"\nRunning command: {' '.join(cmd)}")
                import subprocess
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

                print(f"\nR STDOUT (first 1000 chars):")
                print(result.stdout[:1000])
                print(f"\nR STDERR (first 1000 chars):")
                print(result.stderr[:1000])

        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()

    def test_r_with_correct_season(self):
        """Test R script with correct season for the date"""
        print("\n" + "=" * 80)
        print("TEST R WITH CORRECT SEASON")
        print("=" * 80)

        # IMPORTANT: Your bets are for 2025, so we need to test with 2025
        # But nflverse might not have 2025 data yet since it's the future
        test_data = {
            'player_name': 'Jeremy Ruckert',
            'sport': 'nfl',
            'season': 2025,  # ← Use 2025 season for Dec 2025 games
            'market_type': 'player last touchdown scorer',
            'event_string': 'Miami Dolphins @ New York Jets',
            'line_value': None,
            'game_date': '2025-12-07'  # From your pending bets
        }

        print(f"Test data:")
        for key, value in test_data.items():
            print(f"  {key}: {value}")

        print(f"\nNote: Testing with actual pending bet data")
        print(f"Game on Dec 7, 2025 belongs to the 2025 NFL season")

        try:
            r_result = self.real_stats_resolver._call_r_script(**test_data)

            print(f"\nR SCRIPT RESULT:")
            print(json.dumps(r_result, indent=2))

            if r_result.get('success') and r_result.get('game_id'):
                print(f"\n✓ Found game ID: {r_result['game_id']}")
            else:
                print(f"\n✗ Could not find game. Error: {r_result.get('error', 'Unknown error')}")
                print("\nThis is expected if the game hasn't happened yet or 2025 data isn't available.")

        except Exception as e:
            print(f"Error: {e}")
    def check_pending_bet_dates(self):
        """Check if pending bets are for past or future games"""
        print("\n" + "=" * 80)
        print("CHECKING PENDING BET DATES")
        print("=" * 80)

        pending_bets = self.db.get_pending_bets(limit=10)

        if not pending_bets:
            print("No pending bets")
            return

        today = datetime.now().date()

        for i, bet in enumerate(pending_bets):
            event = bet['event']
            player = bet.get('player', 'Unknown')
            game_date = bet.get('game_date')

            if game_date:
                try:
                    game_date_obj = datetime.strptime(game_date, "%Y-%m-%d").date()
                    status = "PAST" if game_date_obj < today else "FUTURE" if game_date_obj > today else "TODAY"

                    print(f"\nBet #{i + 1}: {player}")
                    print(f"  Event: {event}")
                    print(f"  Game Date: {game_date}")
                    print(f"  Status: {status} ({game_date_obj})")

                    # For past games, check if we should be able to resolve
                    if game_date_obj < today:
                        print(f"  Should be resolvable (game already happened)")

                        # Check what season this belongs to
                        if "Football" in bet.get('sport', '') or bet.get('sport', '').lower() == 'nfl':
                            season = game_date_obj.year - 1 if game_date_obj.month <= 2 else game_date_obj.year
                            print(f"  NFL season: {season} (game in {game_date_obj.month}/{game_date_obj.year})")
                            print(f"  Should use season {season} in R script")

                except Exception as e:
                    print(f"\nBet #{i + 1}: {player}")
                    print(f"  Event: {event}")
                    print(f"  Could not parse date '{game_date}': {e}")
            else:
                print(f"\nBet #{i + 1}: {player}")
                print(f"  Event: {event}")
                print(f"  NO GAME DATE FOUND IN DATABASE!")

    def check_r_packages(self):
        """Check if required R packages are installed"""
        print("\n" + "=" * 80)
        print("CHECKING R PACKAGES")
        print("=" * 80)

        test_r_code = '''
        packages <- c("nflreadr", "hoopR", "jsonlite", "stringr", "dplyr", "lubridate")
        for (pkg in packages) {
          if (!requireNamespace(pkg, quietly = TRUE)) {
            cat(sprintf("Missing package: %s\\n", pkg))
          } else {
            cat(sprintf("Package available: %s\\n", pkg))
          }
        }

        # Test nflreadr specifically
        if (requireNamespace("nflreadr", quietly = TRUE)) {
          cat("Testing nflreadr...\\n")
          tryCatch({
            schedules <- nflreadr::load_schedules(2023)
            cat(sprintf("Successfully loaded %d games from 2023\\n", nrow(schedules)))
            cat("First few games:\\n")
            print(head(schedules[, c("game_id", "gameday", "home_team", "away_team")]))
          }, error = function(e) {
            cat(sprintf("Error loading schedules: %s\\n", e$message))
          })
        }
        '''

        # Write to temp file
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.R', delete=False) as f:
            f.write(test_r_code)
            temp_file = f.name

        try:
            cmd = [self.real_stats_resolver.rscript_path, temp_file]
            print(f"Running: {' '.join(cmd)}")

            import subprocess
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

            print(f"\nR OUTPUT:")
            print(result.stdout)
            if result.stderr:
                print(f"\nR ERRORS:")
                print(result.stderr)

        except Exception as e:
            print(f"Error: {e}")
        finally:
            import os
            if os.path.exists(temp_file):
                os.remove(temp_file)

    def debug_nfl_schedule_lookup(self):
        """Debug: Look up specific NFL game in schedule"""
        print("\n" + "=" * 80)
        print("DEBUG: NFL SCHEDULE LOOKUP")
        print("=" * 80)

        test_r_code = '''
        library(nflreadr)
        library(dplyr)
        library(lubridate)
        library(jsonlite)

        cat("Testing NFL schedule lookup...\\n")

        # Test loading 2023 schedule
        schedules <- load_schedules(2023)
        cat(sprintf("Loaded %d games for 2023\\n", nrow(schedules)))

        # Look for Chiefs vs Dolphins games
        cat("\\nLooking for Chiefs vs Dolphins games in 2023:\\n")
        chiefs_dolphins <- schedules %>%
          filter((home_team == "KC" & away_team == "MIA") | 
                 (home_team == "MIA" & away_team == "KC"))

        if (nrow(chiefs_dolphins) > 0) {
          cat(sprintf("Found %d Chiefs vs Dolphins games:\\n", nrow(chiefs_dolphins)))
          print(chiefs_dolphins[, c("game_id", "gameday", "week", "home_team", "away_team")])

          # Check for Jan 13, 2024 game
          cat("\\nLooking for game on Jan 13, 2024:\\n")
          jan_13_game <- schedules %>% filter(gameday == "2024-01-13")
          if (nrow(jan_13_game) > 0) {
            cat("Games on Jan 13, 2024:\\n")
            print(jan_13_game[, c("game_id", "gameday", "week", "home_team", "away_team")])
          } else {
            cat("No games found on Jan 13, 2024\\n")
          }
        } else {
          cat("No Chiefs vs Dolphins games found in 2023\\n")
        }

        # Test 2024 schedule
        cat("\\n\\nTesting 2024 schedule...\\n")
        try({
          schedules_2024 <- load_schedules(2024)
          cat(sprintf("Loaded %d games for 2024\\n", nrow(schedules_2024)))
        }, error = function(e) {
          cat(sprintf("Error loading 2024: %s\\n", e$message))
        })

        # Show team abbreviations
        cat("\\n\\nTeam abbreviations in schedule:\\n")
        unique_teams <- unique(c(schedules$home_team, schedules$away_team))
        cat(paste(sort(unique_teams), collapse=", "))
        '''

        # Write to temp file
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.R', delete=False) as f:
            f.write(test_r_code)
            temp_file = f.name

        try:
            cmd = [self.real_stats_resolver.rscript_path, temp_file]
            print(f"Running: {' '.join(cmd)}")

            import subprocess
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

            print(f"\nR OUTPUT:")
            print(result.stdout)
            if result.stderr:
                print(f"\nR ERRORS:")
                print(result.stderr)

        except Exception as e:
            print(f"Error: {e}")
        finally:
            import os
            if os.path.exists(temp_file):
                os.remove(temp_file)

    # Update your run() method to include the new debug options:
    def debug_schedule_load_issue(self):
        """Deep diagnostic for nflreadr schedule loading failure - FIXED VERSION"""
        print("\n" + "=" * 80)
        print("DEEP DIAGNOSTIC: NFL SCHEDULE LOADING (FIXED)")
        print("=" * 80)

        test_r_code = '''
        # Set warning handling - DON'T convert warnings to errors for package version warnings
        old_warn <- getOption("warn")
        options(warn = 1)  # Show warnings but don't stop execution
        options(warning.length = 1000L)

        library(nflreadr)
        library(dplyr)
        library(jsonlite)

        cat("=== DIAGNOSTIC START ===\\n")
        cat("System date:", as.character(Sys.Date()), "\\n")
        cat("R version:", R.version.string, "\\n")
        cat("nflreadr version:", as.character(packageVersion("nflreadr")), "\\n\\n")

        # Test 0: Check if we can even load the package without version warnings
        cat("0. Testing package loading...\\n")
        tryCatch({
            # Force reload to see warnings
            detach("package:nflreadr", unload = TRUE)
            library(nflreadr, quietly = FALSE)
            cat("   SUCCESS: nflreadr loaded\\n")
        }, warning = function(w) {
            cat("   WARNING during load:", w$message, "\\n")
        }, error = function(e) {
            cat("   ERROR during load:", e$message, "\\n")
        })

        # Test 1: Check available seasons via the package's internal function
        cat("\\n1. Checking available seasons...\\n")
        tryCatch({
            # Try the public available_seasons() function first
            if ("available_seasons" %in% ls(getNamespace("nflreadr"))) {
                seasons <- nflreadr::available_seasons()
                cat("   Available seasons (public function):", paste(seasons, collapse=", "), "\\n")
            } else {
                cat("   available_seasons() function not found\\n")
            }

            # Try to see what the package data shows
            cat("   Trying to guess available seasons from data...\\n")
            # Check if 2023 loads
            tryCatch({
                sched_2023 <- nflreadr::load_schedules(2023)
                cat("   ✓ 2023 schedule loaded:", nrow(sched_2023), "games\\n")
            }, error = function(e) {
                cat("   ✗ 2023 failed:", e$message, "\\n")
            })

            # Check if 2024 loads
            tryCatch({
                sched_2024 <- nflreadr::load_schedules(2024)
                cat("   ✓ 2024 schedule loaded:", nrow(sched_2024), "games\\n")
            }, error = function(e) {
                cat("   ✗ 2024 failed:", e$message, "\\n")
            })

            # Check if 2025 loads
            tryCatch({
                sched_2025 <- nflreadr::load_schedules(2025)
                cat("   ✓ 2025 schedule loaded:", nrow(sched_2025), "games\\n")
                cat("   Latest game in 2025:", max(sched_2025$gameday), "\\n")
            }, error = function(e) {
                cat("   ✗ 2025 failed:", e$message, "\\n")
            })

        }, error = function(e) {
            cat("   ERROR checking seasons:", e$message, "\\n")
        })

        # Test 2: Try to directly download the schedule data
        cat("\\n2. Direct data source test...\\n")
        # Try multiple possible URLs
        urls_to_test <- list(
            "https://github.com/nflverse/nfldata/raw/master/data/schedules/sched_2025.rds",
            "https://github.com/nflverse/nflverse-data/releases/download/schedules/schedules_2025.rds",
            "https://github.com/nflverse/nflverse-data/releases/download/schedules/schedule_2025.rds"
        )

        for (i in seq_along(urls_to_test)) {
            url <- urls_to_test[[i]]
            cat("   URL", i, ":", url, "\\n")

            temp_file <- tempfile(fileext = ".rds")
            tryCatch({
                # Use try() to avoid stopping on download errors
                download_result <- try({
                    utils::download.file(url, temp_file, quiet = TRUE, mode = "wb")
                }, silent = TRUE)

                if (!inherits(download_result, "try-error") && file.exists(temp_file) && file.size(temp_file) > 0) {
                    cat("   ✓ Downloaded successfully\\n")
                    # Try to read it
                    sched_data <- try(readRDS(temp_file), silent = TRUE)
                    if (!inherits(sched_data, "try-error")) {
                        cat("   ✓ RDS file loaded.", nrow(sched_data), "rows.\\n")
                        # Look for our specific game
                        target_game <- sched_data %>%
                            filter(gameday == "2025-12-07" &
                                   ((home_team == "NYJ" & away_team == "MIA") |
                                    (home_team == "MIA" & away_team == "NYJ")))
                        if (nrow(target_game) > 0) {
                            cat("   ✓ FOUND Dolphins @ Jets game! Game ID:", target_game$game_id[1], "\\n")
                            break  # Stop testing URLs once we find the data
                        } else {
                            cat("   ✗ Game not found in this data\\n")
                        }
                    } else {
                        cat("   ✗ Could not read RDS file\\n")
                    }
                } else {
                    cat("   ✗ Download failed or file empty\\n")
                }

                # Clean up temp file
                if (file.exists(temp_file)) unlink(temp_file)

            }, error = function(e) {
                cat("   ✗ Error:", e$message, "\\n")
                if (file.exists(temp_file)) unlink(temp_file)
            })
        }

        # Test 3: Manual data check for Dolphins @ Jets on 2025-12-07
        cat("\\n3. Manual game lookup for Dolphins @ Jets on 2025-12-07...\\n")
        # First, see if we can find ANY 2025 data
        tryCatch({
            # Try to get 2025 data any way we can
            all_2025_data <- FALSE

            # Method 1: Try load_schedules with suppressWarnings
            sched_2025 <- try(suppressWarnings(nflreadr::load_schedules(2025)), silent = TRUE)

            if (!inherits(sched_2025, "try-error") && nrow(sched_2025) > 0) {
                cat("   ✓ Got 2025 schedule via load_schedules()\\n")
                all_2025_data <- TRUE
            } else {
                cat("   ✗ load_schedules(2025) returned no data\\n")

                # Method 2: Try to load from known file patterns
                cat("   Trying alternative data sources...\\n")
                # You might need to manually check the nflverse-data GitHub repo
                cat("   Manual check needed: Visit https://github.com/nflverse/nflverse-data\\n")
                cat("   Look for 'schedules' folder or releases with 2025 data\\n")
            }

            if (all_2025_data) {
                # Search for our game
                target_game <- sched_2025 %>%
                    filter(gameday == "2025-12-07" &
                           ((home_team == "NYJ" & away_team == "MIA") |
                            (home_team == "MIA" & away_team == "NYJ")))

                if (nrow(target_game) > 0) {
                    cat("   ✓ SUCCESS! Found game:", target_game$game_id[1], "\\n")
                    cat("   Home:", target_game$home_team[1], "Away:", target_game$away_team[1], "\\n")
                } else {
                    cat("   ✗ Game not found in 2025 schedule\\n")
                    cat("   Sample of 2025 games near that date:\\n")
                    near_games <- sched_2025 %>%
                        filter(abs(as.Date(gameday) - as.Date("2025-12-07")) <= 7) %>%
                        arrange(gameday)
                    if (nrow(near_games) > 0) {
                        print(head(near_games[, c("game_id", "gameday", "home_team", "away_team")]))
                    } else {
                        cat("   No games found within 7 days of 2025-12-07\\n")
                    }
                }
            }

        }, error = function(e) {
            cat("   ✗ Error in manual lookup:", e$message, "\\n")
        })

        # Test 4: Check what data IS available
        cat("\\n4. Checking what seasons ARE available...\\n")
        # Try a range of years
        years_to_check <- c(2020, 2021, 2022, 2023, 2024, 2025, 2026)
        available_years <- c()

        for (year in years_to_check) {
            tryCatch({
                sched <- suppressWarnings(nflreadr::load_schedules(year))
                if (nrow(sched) > 0) {
                    cat("   ✓", year, ":", nrow(sched), "games (latest:", max(sched$gameday), ")\\n")
                    available_years <- c(available_years, year)
                } else {
                    cat("   ✗", year, ": No data\\n")
                }
            }, error = function(e) {
                cat("   ✗", year, ": Error -", e$message, "\\n")
            })
        }

        cat("\\n   Summary: Available years -", paste(available_years, collapse=", "), "\\n")

        cat("\\n=== DIAGNOSTIC END ===\\n")
        cat("\\nKEY FINDING: If 2025 is not in available years, nflverse does not have 2025 data yet.\\n")
        cat("Today is", as.character(Sys.Date()), "- games from Dec 7, 2025 should be available.\\n")
        cat("This suggests either:\\n")
        cat("1. nflverse hasn't updated with 2025 data (unlikely since season is ongoing)\\n")
        cat("2. Your nflreadr package is outdated\\n")
        cat("3. There's a network/data access issue\\n")

        # Restore original warning setting
        options(warn = old_warn)

        # Return minimal JSON to keep Python happy
        cat(toJSON(list(diagnostic_complete = TRUE)))
        '''

        # Write to temp file
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.R', delete=False, encoding='utf-8') as f:
            f.write(test_r_code)
            temp_file = f.name

        try:
            cmd = [self.real_stats_resolver.rscript_path, temp_file]
            print(f"Running: {' '.join(cmd)}")

            import subprocess
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=45)

            print(f"\nR OUTPUT:")
            print(result.stdout)
            if result.stderr:
                print(f"\nR STDERR (Errors/Warnings):")
                print(result.stderr)

        except Exception as e:
            print(f"Error: {e}")
        finally:
            import os
            if os.path.exists(temp_file):
                os.remove(temp_file)

    def test_exact_pending_bet(self):
        """Test R script with EXACT data from a pending bet"""
        print("\n" + "=" * 80)
        print("TEST EXACT PENDING BET RESOLUTION")
        print("=" * 80)

        # Get a specific pending bet
        pending_bets = self.db.get_pending_bets(limit=5)
        if not pending_bets:
            print("No pending bets found")
            return

        # Use the first bet (Jeremy Ruckert - Player Last Touchdown Scorer)
        bet = pending_bets[0]
        print(f"Testing with bet: {bet.get('player', 'Unknown')}")
        print(f"Event: {bet['event']}")
        print(f"Game Date: {bet.get('game_date', 'Unknown')}")
        print(f"Market: {bet.get('market', 'Unknown')}")

        # Extract components from the bet
        player_name = bet.get('player', '')
        sport = bet.get('sport', '').lower()
        event_string = bet['event']
        game_date = bet.get('game_date')
        market = bet.get('market', '')

        # Determine market type
        market_lower = market.lower()
        if "first" in market_lower and ("scorer" in market_lower or "td" in market_lower):
            market_type = "first_scorer"
        elif "last" in market_lower and ("scorer" in market_lower or "td" in market_lower):
            market_type = "last_scorer"
        else:
            market_type = "player_prop"

        # Extract season - CRITICAL!
        # Since game_date is 2025-12-07, and it's NFL:
        # NFL season 2025 = games from Sep 2025 to Feb 2026
        season = 2025

        print(f"\nCalling R script with:")
        print(f"  Player: {player_name}")
        print(f"  Sport: {sport}")
        print(f"  Season: {season} ← MUST BE 2025 FOR DEC 2025 GAME")
        print(f"  Market Type: {market_type}")
        print(f"  Event: {event_string}")
        print(f"  Game Date: {game_date}")

        try:
            # Call R directly
            r_result = self.real_stats_resolver._call_r_script(
                player_name=player_name,
                sport=sport,
                season=season,
                market_type=market_type,
                event_string=event_string,
                line_value=None,
                game_date=game_date
            )

            print(f"\nR SCRIPT RESULT:")
            print(json.dumps(r_result, indent=2))

            # Analyze the result
            if r_result.get('success'):
                if r_result.get('resolved'):
                    print(f"\n✅ SUCCESS! Bet resolved!")
                    if 'bet_won' in r_result:
                        print(f"   Result: {'WIN' if r_result['bet_won'] else 'LOSS'}")
                    if 'actual_value' in r_result:
                        print(f"   Actual value: {r_result['actual_value']}")
                else:
                    print(f"\n⚠ Bet not resolved (player may not have stats)")
                    print(f"   Error: {r_result.get('error', 'No error')}")
                    if 'game_id' in r_result:
                        print(f"   Game ID found: {r_result['game_id']}")
            else:
                print(f"\n❌ R script failed")
                print(f"   Error: {r_result.get('error', 'Unknown error')}")

                # If it says "Could not find game ID", there's a bug in your R script
                if "Could not find game ID" in r_result.get('error', ''):
                    print(f"\n🔍 DEBUGGING: The diagnostic found game 2025_14_MIA_NYJ")
                    print(f"   But your R script can't find it. Possible issues:")
                    print(f"   1. Team name matching in R script is broken")
                    print(f"   2. Date parsing in R script is wrong")
                    print(f"   3. Season parameter is wrong")

        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()

    def debug_r_script_directly(self):
        """Debug the actual R script with verbose output"""
        print("\n" + "=" * 80)
        print("DEBUG R SCRIPT DIRECTLY")
        print("=" * 80)

        # Create a test R script that mimics what your bet_resolver.R does
        test_r_code = '''
        # Turn on ALL debugging
        options(warn = 1)

        library(nflreadr)
        library(dplyr)
        library(stringr)
        library(jsonlite)

        cat("=== DIRECT TEST OF bet_resolver.R LOGIC ===\\n")

        # Simulate what your R script should do
        player_name <- "Jeremy Ruckert"
        sport <- "nfl"
        season <- 2025
        market_type <- "last_scorer"
        event_string <- "Miami Dolphins @ New York Jets"
        game_date <- "2025-12-07"

        cat("\\nInput parameters:\\n")
        cat("  Player:", player_name, "\\n")
        cat("  Sport:", sport, "\\n")
        cat("  Season:", season, "\\n")
        cat("  Market:", market_type, "\\n")
        cat("  Event:", event_string, "\\n")
        cat("  Game Date:", game_date, "\\n")

        # Step 1: Load schedule
        cat("\\n--- Step 1: Loading schedule for season", season, "---\\n")
        tryCatch({
            schedules <- nflreadr::load_schedules(season)
            cat("✓ Successfully loaded", nrow(schedules), "games\\n")
            cat("  Date range:", min(schedules$gameday), "to", max(schedules$gameday), "\\n")
        }, error = function(e) {
            cat("✗ Error loading schedule:", e$message, "\\n")
        })

        # Step 2: Find the specific game
        cat("\\n--- Step 2: Looking for game ---\\n")
        cat("Looking for: Dolphins @ Jets on", game_date, "\\n")

        # Try to extract teams (simplified version)
        teams <- str_split(event_string, " @ ")[[1]]
        if (length(teams) == 2) {
            away_team_raw <- teams[1]
            home_team_raw <- teams[2]
            cat("  Extracted teams:", away_team_raw, "@", home_team_raw, "\\n")
        } else {
            cat("  Could not extract teams from event string\\n")
        }

        # Look for the game
        target_game <- schedules %>%
            filter(gameday == game_date) %>%
            filter((home_team == "NYJ" & away_team == "MIA") |
                   (home_team == "MIA" & away_team == "NYJ"))

        if (nrow(target_game) > 0) {
            cat("✓ FOUND GAME!\\n")
            cat("  Game ID:", target_game$game_id[1], "\\n")
            cat("  Home:", target_game$home_team[1], "\\n")
            cat("  Away:", target_game$away_team[1], "\\n")
            cat("  Week:", target_game$week[1], "\\n")

            # Step 3: Try to load player stats
            cat("\\n--- Step 3: Loading player stats ---\\n")
            game_id <- target_game$game_id[1]

            tryCatch({
                # Try to load player stats
                player_stats_df <- nflreadr::load_player_stats(seasons = season)
                cat("✓ Loaded player stats:", nrow(player_stats_df), "rows\\n")

                # DEBUG: Check column names
                cat("  DEBUG: Checking columns...\\n")
                cat("  First few column names:\\n")
                print(head(colnames(player_stats_df), 10))

                # Look for player name column
                player_col <- NULL
                possible_cols <- c("player_display_name", "player_name", "display_name", 
                                   "full_name", "name", "athlete_display_name")

                for (col in possible_cols) {
                    if (col %in% colnames(player_stats_df)) {
                        player_col <- col
                        cat("  Using column:", player_col, "for player matching\\n")
                        break
                    }
                }

                if (is.null(player_col)) {
                    cat("✗ No player name column found!\\n")
                    result <- list(
                        success = FALSE,
                        error = "No player name column in dataset"
                    )
                } else {
                    # Use pattern matching to find player
                    player_pattern <- paste0("(?i)", gsub(" ", ".*", player_name))
                    player_data <- player_stats_df %>%
                        filter(grepl(player_pattern, .data[[player_col]])) %>%
                        filter(game_id == !!game_id)

                    if (nrow(player_data) > 0) {
                        cat("✓ FOUND PLAYER STATS!\\n")
                        cat("  Player:", player_data[[player_col]][1], "\\n")
                        cat("  Receptions:", player_data$receptions[1], "\\n")
                        cat("  Receiving Yards:", player_data$receiving_yards[1], "\\n")
                        cat("  Receiving TDs:", player_data$receiving_tds[1], "\\n")

                        # Check if they scored
                        scored_td <- player_data$receiving_tds[1] > 0
                        cat("  Scored TD?:", ifelse(scored_td, "YES", "NO"), "\\n")

                        result <- list(
                            success = TRUE,
                            resolved = TRUE,
                            game_id = game_id,
                            player_scored = scored_td,
                            stats = list(
                                receptions = player_data$receptions[1],
                                receiving_yards = player_data$receiving_yards[1],
                                receiving_tds = player_data$receiving_tds[1]
                            )
                        )
                    } else {
                        cat("✗ Player not found in stats for this game\\n")
                        cat("  DEBUG: Showing sample player names...\\n")
                        sample_players <- unique(player_stats_df[[player_col]])[1:5]
                        for (p in sample_players) {
                            cat("    -", p, "\\n")
                        }

                        result <- list(
                            success = TRUE,
                            resolved = FALSE,
                            game_id = game_id,
                            error = paste("Player", player_name, "not found in game", game_id)
                        )
                    }
                }

            }, error = function(e) {
                cat("✗ Error loading player stats:", e$message, "\\n")
                result <- list(
                    success = FALSE,
                    error = paste("Failed to load player stats:", e$message)
                )
            })

        } else {
            cat("✗ GAME NOT FOUND!\\n")
            cat("  Games on", game_date, ":\\n")
            games_on_date <- schedules %>% filter(gameday == game_date)
            if (nrow(games_on_date) > 0) {
                print(games_on_date[, c("game_id", "home_team", "away_team")])
            } else {
                cat("  No games on this date\\n")
            }

            result <- list(
                success = FALSE,
                error = paste("Could not find game:", event_string, "on", game_date)
            )
        }

        cat("\\n=== TEST COMPLETE ===\\n")
        cat(toJSON(result, auto_unbox = TRUE))
        '''

        # Write to temp file
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.R', delete=False, encoding='utf-8') as f:
            f.write(test_r_code)
            temp_file = f.name

        try:
            cmd = [self.real_stats_resolver.rscript_path, temp_file]
            print(f"Running: {' '.join(cmd)}")

            import subprocess
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=45)

            print(f"\nR OUTPUT:")
            print(result.stdout)
            if result.stderr:
                print(f"\nR STDERR (Errors/Warnings):")
                print(result.stderr)

        except Exception as e:
            print(f"Error: {e}")
        finally:
            import os
            if os.path.exists(temp_file):
                os.remove(temp_file)

    def _should_resolve_bet(self, bet: Dict) -> bool:
        """Enhanced version with better debugging"""
        player = bet.get("player") or bet.get("player_name", "")
        game_date = bet.get('game_date')

        self.logger.info(f"DEBUG _should_resolve_bet called for: {player}")
        self.logger.info(f"DEBUG Game date in bet: {game_date}")

        # Check if game date is in the past
        if game_date:
            try:
                game_date_obj = datetime.strptime(game_date, "%Y-%m-%d").date()
                current_date = datetime.now().date()

                if game_date_obj > current_date:
                    self.logger.info(f"DEBUG Game date {game_date} is in the future. Waiting...")
                    return False
                elif game_date_obj <= current_date:
                    self.logger.info(f"DEBUG Game date {game_date} is in the past. Should resolve.")
                    return True
            except Exception as e:
                self.logger.warning(f"DEBUG Could not parse game date {game_date}: {e}")

        # If no game date, check if event has a date
        event = bet.get("event", "")
        if event:
            # Try to extract date from event string
            date_patterns = [
                r'(\d{4}-\d{2}-\d{2})',
                r'(\d{1,2}/\d{1,2}/\d{4})'
            ]

            for pattern in date_patterns:
                match = re.search(pattern, event)
                if match:
                    date_str = match.group(1)
                    try:
                        if '/' in date_str:
                            # Handle MM/DD/YYYY
                            month, day, year = date_str.split('/')
                            date_str = f"{year}-{month.zfill(2)}-{day.zfill(2)}"

                        game_date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
                        current_date = datetime.now().date()

                        if game_date_obj > current_date:
                            self.logger.info(f"DEBUG Extracted game date {date_str} is in the future. Waiting...")
                            return False
                        elif game_date_obj <= current_date:
                            self.logger.info(f"DEBUG Extracted game date {date_str} is in the past. Should resolve.")
                            return True

                    except Exception as e:
                        self.logger.warning(f"DEBUG Could not parse extracted date {date_str}: {e}")

        # Default: allow resolution if we can't determine the date
        self.logger.info("DEBUG Could not determine game date. Allowing resolution attempt.")
        return True

# =============================================================================
# RUN THE ENHANCED SYSTEM
# =============================================================================

if __name__ == "__main__":
    print("Starting ENHANCED Paper Trading System...")

    # Validate configuration first
    if not config.validate():
        print("Configuration validation failed. Please check your settings.")
        print("Create a config.json file or set environment variables:")
        print("   - MRDOGE_API_KEY=your_api_key_here")
        print("   - INITIAL_BANKROLL=3000")
        if config.database.use_supabase:
            print("   - SUPABASE_URL=your_supabase_url")
            print("   - SUPABASE_KEY=your_supabase_key")
        sys.exit(1)

    system = EnhancedPaperTradingSystem()
    system.run()