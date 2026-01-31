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


# =============================================================================
# PRODUCTION-GRADE R STATS RESOLVER - UPDATED WITH SPORT NORMALIZATION
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
    # 1. SPORT NORMALIZATION (CRITICAL FIX)
    # ------------------------------------------------------------------
    def _normalize_sport(self, sport: str) -> str:
        """Normalize sport names for R script"""
        if not sport:
            return "unknown"

        sport_lower = sport.lower().strip()

        # Map various names to what R expects
        sport_mapping = {
            'football': 'nfl',
            'nfl': 'nfl',
            'nfl football': 'nfl',
            'american football': 'nfl',
            'basketball': 'nba',
            'nba': 'nba',
            'nba basketball': 'nba',
            'baseball': 'mlb',
            'mlb': 'mlb',
            'hockey': 'nhl',
            'nhl': 'nhl'
        }

        for key, value in sport_mapping.items():
            if sport_lower == key or sport_lower.startswith(key):
                self.logger.debug(f"Sport normalized: '{sport}' -> '{value}'")
                return value

        self.logger.debug(f"Sport not normalized: '{sport}' -> '{sport_lower}'")
        return sport_lower

    # ------------------------------------------------------------------
    # 2. RESOLUTION GATING (Fixed for future games)
    # ------------------------------------------------------------------
    def _should_resolve_bet(self, bet: Dict) -> bool:
        """Check if a bet should be resolved based on game date"""
        player = bet.get("player") or bet.get("player_name", "")
        game_date = bet.get('game_date')

        self.logger.info(f"Checking resolution for: {player}, game_date: {game_date}")

        if not game_date:
            self.logger.warning(f"No game_date for bet: {player}")
            return False  # Can't resolve without date

        try:
            # Parse game date
            if isinstance(game_date, str):
                game_date_obj = datetime.strptime(game_date, "%Y-%m-%d").date()
            else:
                # Assume it's already a date object
                game_date_obj = game_date

            current_date = datetime.now().date()

            # If game is in the future, don't attempt resolution
            if game_date_obj > current_date:
                self.logger.info(f"Game {game_date_obj} is in the future. Skipping.")
                return False

            # If game is today, check if enough time has passed (at least 6 hours after start)
            if game_date_obj == current_date:
                current_hour = datetime.now().hour
                # Most games end by midnight local time
                if current_hour < 6:  # Before 6 AM next day
                    self.logger.info(f"Game today {game_date_obj}, waiting for completion.")
                    return False

            # Games in the past are OK to resolve
            self.logger.info(f"Game {game_date_obj} is in the past. Should resolve.")
            return True

        except Exception as e:
            self.logger.error(f"Error parsing game_date {game_date}: {e}")
            return False

    # ------------------------------------------------------------------
    # 3. MAIN RESOLUTION ENTRY POINT (Updated with sport normalization)
    # ------------------------------------------------------------------
    def _clean_player_and_extract_line(self, player_str: str, market_str: str = "") -> Tuple[
        str, Optional[float], Optional[str]]:
        """
        Clean player names that contain line values and extract line value.
        Returns: (cleaned_player_name, line_value, over_under_direction)

        Examples:
            "Michael Wilson Over 4.5" → ("Michael Wilson", 4.5, "over")
            "C.J. Stroud Under 0.5" → ("C.J. Stroud", 0.5, "under")
            "Shedeur Sanders Under 0.5" → ("Shedeur Sanders", 0.5, "under")
            "Patrick Mahomes" → ("Patrick Mahomes", None, None)
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

                    # Use pattern direction if available, otherwise check for "over"/"under" in string
                    if pattern_direction:
                        direction = pattern_direction
                    elif re.search(r'\b[Oo]ver\b', player_original):
                        direction = "over"
                    elif re.search(r'\b[Uu]nder\b', player_original):
                        direction = "under"

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
    def resolve_player_prop(self, bet: Dict) -> Tuple[bool, float]:
        """
        Main resolution with proper retry tracking and R as source of truth
        """
        try:
            # ========== ADD COMPREHENSIVE DEBUG ==========
            print(f"\n[DEBUG resolve_player_prop] Bet ID: {bet.get('id')}")
            print(f"  All bet keys: {list(bet.keys())}")

            # Debug ALL fields to see what's actually in the bet
            for key, value in bet.items():
                if value:  # Only print non-empty values
                    print(f"  {key}: '{value}'")

            # ========== CRITICAL FIX: Use ACTUAL DATABASE COLUMN NAMES ==========
            # Your database has: 'player', 'market', 'event' (NOT player_name, market_type, event_string)
            player_raw = bet.get("player", "")  # ← DATABASE COLUMN: 'player'
            sport_raw = bet.get("sport", "").lower()
            market_raw = bet.get("market", "")  # ← DATABASE COLUMN: 'market'
            event = bet.get("event", "")  # ← DATABASE COLUMN: 'event'
            stake = float(bet.get("stake", 0))
            odds = float(bet.get("odds", 0))
            game_date = bet.get('game_date')

            # ========== NEW: NORMALIZE SPORT FIRST ==========
            sport = self._normalize_sport(sport_raw)
            print(f"[DEBUG] Sport normalized: '{sport_raw}' → '{sport}'")

            # ========== NEW: CLEAN PLAYER NAME AND EXTRACT LINE VALUE ==========
            # Some bets have line values in player column, e.g. "Michael Wilson Over 4.5"
            player_clean, line_value_from_player, over_under_direction = self._clean_player_and_extract_line(player_raw,
                                                                                                             market_raw)
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
            print(f"  sport: '{sport_raw}' → normalized: '{sport}'")
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
                raise Exception("Resolution not yet appropriate (game may be in future or today)")

            # Determine market type
            market_lower = market_raw.lower()
            print(f"[DEBUG] Market string: '{market_raw}'")
            print(f"[DEBUG] Market lower: '{market_lower}'")

            if any(keyword in market_lower for keyword in ["first", "last", "scorer"]):
                market_type = self._determine_special_market_type(market_raw)
                print(f"[DEBUG] Special market detected: '{market_raw}' -> '{market_type}'")
            else:
                market_type = "player_prop"
                print(f"[DEBUG] Standard player prop market")

            # ========== ADD OVER/UNDER DIRECTION TO MARKET TYPE IF NEEDED ==========
            # If market type doesn't have direction but we detected one, add it
            if over_under_direction and "over" not in market_lower and "under" not in market_lower:
                # Check what kind of market it is
                if "reception" in market_lower:
                    market_type = f"player receptions {over_under_direction}"
                elif "touchdown" in market_lower or "td" in market_lower:
                    market_type = f"player touchdowns {over_under_direction}"
                elif "passing" in market_lower:
                    market_type = f"player passing {over_under_direction}"
                elif "yards" in market_lower:
                    if "receiving" in market_lower:
                        market_type = f"player receiving yards {over_under_direction}"
                    elif "rushing" in market_lower:
                        market_type = f"player rushing yards {over_under_direction}"
                    elif "passing" in market_lower:
                        market_type = f"player passing yards {over_under_direction}"
                    else:
                        market_type = f"player yards {over_under_direction}"
                else:
                    market_type = f"{market_raw} {over_under_direction}"
                print(f"[DEBUG] Added direction to market type: '{market_type}'")

            # ========== EXTRACT SEASON FROM GAME_DATE ==========
            season = self._extract_season_hint(event, sport, game_date)

            # DEBUG season calculation
            print(f"[DEBUG] Season calculation:")
            print(f"  game_date: '{game_date}'")
            print(f"  normalized sport: '{sport}'")
            print(f"  calculated season: {season}")

            print(f"\n[DEBUG] FINAL PARAMETERS FOR R SCRIPT:")
            print(f"  player_name='{player}' (cleaned)")
            print(f"  sport='{sport}' (normalized: '{sport_raw}' -> '{sport}')")
            print(f"  season={season} (calculated from game_date)")
            print(f"  market_type='{market_type}' (converted from: '{market_raw}')")
            print(f"  event_string='{event}'")
            print(f"  line_value={line_value} (extracted from player/market)")
            print(f"  game_date='{game_date}'")

            # ========== Call R with 7 parameters ==========
            r_result = self._call_r_script(
                player_name=player,  # Use CLEANED player name
                sport=sport,  # Use NORMALIZED sport
                season=season,
                market_type=market_type,
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

            # Process based on market type
            if market_type in ("first_scorer", "last_scorer"):
                won, profit = self._resolve_special_market(bet, r_result)
            else:
                won, profit = self._resolve_standard_prop(bet, r_result, line_value, over_under_direction)

            self.logger.info(f"{'WIN' if won else 'LOSS'}: {player} - €{profit:+.2f}")
            print(f"[SUCCESS] Bet resolved: won={won}, profit={profit}")

            return won, profit

        except Exception as e:
            self.logger.warning(f"Resolution failed: {e}")
            print(f"[EXCEPTION] resolve_player_prop failed: {e}")
            import traceback
            traceback.print_exc()
            raise

    # ------------------------------------------------------------------
    # 4. R INVOCATION WITH PRODUCTION SAFEGUARDS (Updated)
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
                sport,  # Argument 2 - R expects "sport" (now normalized)
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

            # Debug output
            self._debug_r_communication(cmd, result)

            # Parse R output - LOOK IN BOTH STDOUT AND STDERR
            if result.returncode == 0:
                # Check stdout first
                json_line = None

                # Try to find JSON in stdout
                for line in result.stdout.strip().split("\n"):
                    line = line.strip()
                    if line.startswith("{") and line.endswith("}"):
                        json_line = line
                        break

                # If not found in stdout, try stderr
                if not json_line:
                    for line in result.stderr.strip().split("\n"):
                        line = line.strip()
                        if line.startswith("{") and line.endswith("}"):
                            json_line = line
                            break

                if not json_line:
                    # If no JSON found, try to extract from any output
                    all_output = result.stdout + result.stderr
                    import re
                    json_match = re.search(r'(\{.*\})', all_output, re.DOTALL)
                    if json_match:
                        json_line = json_match.group(1)

                if not json_line:
                    self.logger.error(f"No JSON from R. Stdout: {result.stdout[:200]}, Stderr: {result.stderr[:200]}")
                    return {"success": False, "error": "No valid JSON from R"}

                try:
                    data = json.loads(json_line)
                    self.r_successful += 1

                    # Cache successful results
                    if data.get("success") and data.get("resolved"):
                        # Use cache_key directly since we removed bet_key parameter
                        self._set_cached_result(cache_key, data, ttl_hours=24)

                    return data

                except json.JSONDecodeError as e:
                    self.logger.error(f"JSON decode error: {e}")
                    self.logger.error(f"JSON line that failed: {json_line[:200]}")
                    return {"success": False, "error": f"Invalid JSON from R: {e}"}

            else:
                # R script failed
                error_msg = result.stderr[:300] if result.stderr else result.stdout[:300]
                self.logger.error(f"R script failed (code {result.returncode}): {error_msg}")
                return {"success": False, "error": error_msg}

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
    # 5. HELPER METHODS (Updated with season fix)
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
                    # Example: Jan 2025 game belongs to 2024 season
                    if game_date_obj.month <= 2:
                        return game_date_obj.year - 1
                    else:
                        return game_date_obj.year

                elif sport == "nba" or sport == "basketball":
                    # NBA season: Games Oct-June, spanning years
                    # Example: Oct 2024 game belongs to 2024-25 season → return 2024
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
    # 6. CACHE SYSTEM (Production-grade)
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
    # 7. RESOLUTION LOGIC
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

    def _resolve_standard_prop(self, bet: Dict, r_result: Dict, line_value: Optional[float],
                               direction: Optional[str] = None) -> Tuple[bool, float]:
        player_name = bet.get("player", "")
        market = bet.get("market", "").lower()
        stake = float(bet.get("stake", 0))
        odds = float(bet.get("odds", 0))

        # Get actual value from R
        actual_value = None
        stats = r_result.get("data", {}).get("stats", {})

        # ========== NEW: Also check the original player field for Over/Under ==========
        original_player = bet.get("player", "").lower()
        has_over_in_player = "over" in original_player or " o " in original_player
        has_under_in_player = "under" in original_player or " u " in original_player

        # Use provided direction if available, otherwise detect from strings
        if direction:
            over_under = direction
        elif "over" in market or has_over_in_player:
            over_under = "over"
        elif "under" in market or has_under_in_player:
            over_under = "under"
        else:
            over_under = "over"  # Default assumption for "any" markets

        # Determine which stat to use based on market
        if "points" in market:
            actual_value = stats.get("points", 0)
        elif "rebound" in market:
            actual_value = stats.get("rebounds", 0)
        elif "assist" in market:
            actual_value = stats.get("assists", 0)
        elif "yards" in market:
            if "passing" in market:
                actual_value = stats.get("passing_yards", 0)
            elif "rushing" in market:
                actual_value = stats.get("rushing_yards", 0)
            elif "receiving" in market:
                actual_value = stats.get("receiving_yards", 0)
            else:
                actual_value = sum(stats.get(key, 0) for key in
                                   ["passing_yards", "rushing_yards", "receiving_yards"])
        elif "reception" in market:
            actual_value = stats.get("receptions", 0)
        elif "touchdown" in market or "td" in market:
            actual_value = stats.get("touchdowns", 0) or stats.get("total_tds", 0)
        elif "passing" in market and "td" in market:
            actual_value = stats.get("passing_tds", 0)

        if actual_value is None or line_value is None:
            raise Exception(f"Cannot resolve market '{market}'")

        # Determine over/under using the detected direction
        if over_under == "over":
            won = actual_value > line_value
            comparison = f"{actual_value} > {line_value} ({over_under})"
        elif over_under == "under":
            won = actual_value < line_value
            comparison = f"{actual_value} < {line_value} ({over_under})"
        else:
            won = actual_value > 0 if line_value is None else actual_value > line_value
            comparison = f"{actual_value} > {line_value if line_value else 0}"

        self.logger.info(f"{player_name}: {comparison} -> {'WIN' if won else 'LOSS'}")

        profit = stake * (odds - 1) if won else -stake
        return won, profit

    # ------------------------------------------------------------------
    # 8. UTILITIES AND MONITORING
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

    def test_sport_normalization(self):
        """Test the sport normalization function"""
        print("\n" + "=" * 80)
        print("TESTING SPORT NORMALIZATION")
        print("=" * 80)

        test_cases = [
            ("football", "nfl"),
            ("Football", "nfl"),
            ("NFL", "nfl"),
            ("NFL Football", "nfl"),
            ("american football", "nfl"),
            ("basketball", "nba"),
            ("Basketball", "nba"),
            ("NBA", "nba"),
            ("NBA Basketball", "nba"),
            ("baseball", "mlb"),
            ("MLB", "mlb"),
            ("hockey", "nhl"),
            ("NHL", "nhl"),
            ("soccer", "soccer"),  # Should not be normalized
            ("", "unknown"),
        ]

        print("Test Cases:")
        print("-" * 40)
        for input_sport, expected in test_cases:
            result = self._normalize_sport(input_sport)
            status = "✓" if result == expected else "✗"
            print(f"{status} '{input_sport}' -> '{result}' (expected: '{expected}')")

        return all(self._normalize_sport(tc[0]) == tc[1] for tc in test_cases)


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
            'manager_errors': 0,
            'batch_id': f"batch_{int(time.time())}",
            'details': []
        }

        for bet in pending_bets:
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

    def teams_match_enhanced(self, event_teams, scraped_teams):
        """ENHANCED: More flexible team matching"""
        # Normalize all team names
        norm_event_teams = [self._normalize_team_name(t) for t in event_teams]
        norm_scraped_teams = [self._normalize_team_name(t) for t in scraped_teams]

        # Check for direct matches
        if set(norm_event_teams) == set(norm_scraped_teams):
            return True

        # Check partial matches (if one team matches and other is close)
        matches = 0
        for e_team in norm_event_teams:
            for s_team in norm_scraped_teams:
                if e_team in s_team or s_team in e_team:
                    matches += 1
                    break

        return matches >= 1  # At least one team matches

    def _normalize_team_name(self, team_name):
        """Normalize team name for comparison"""
        if not team_name:
            return ""

        # Convert to lowercase
        team_lower = team_name.lower()

        # Remove common prefixes/suffixes
        team_lower = re.sub(r'^(the\s+)', '', team_lower)
        team_lower = re.sub(r'\s+(team|club|fc|bb|basketball|football)$', '', team_lower)

        # Remove location prefixes
        team_lower = re.sub(r'^(los angeles|la|new york|ny|san francisco|sf|las vegas|lv)\s+', '', team_lower)

        # Common abbreviations
        abbreviations = {
            'gsw': 'warriors',
            'lal': 'lakers',
            'nyk': 'knicks',
            'mia': 'heat',
            'bos': 'celtics',
            'chi': 'bulls',
            'hou': 'rockets',
            'phx': 'suns',
            'phi': '76ers',
            'dal': 'mavericks',
            'den': 'nuggets',
            'atl': 'hawks',
            'bkn': 'nets',
            'cle': 'cavaliers',
            'ind': 'pacers',
            'mil': 'bucks',
            'orl': 'magic',
            'tor': 'raptors',
            'uta': 'jazz',
            'was': 'wizards',
            'sac': 'kings',
            'sas': 'spurs',
            'okc': 'thunder',
            'mem': 'grizzlies',
            'nop': 'pelicans',
            'cha': 'hornets',
            'min': 'timberwolves',
            'por': 'trail blazers',
            'det': 'pistons',
            'lac': 'clippers'
        }

        # Replace abbreviations
        for abbrev, full_name in abbreviations.items():
            if team_lower == abbrev:
                return full_name

        return team_lower


# =============================================================================
# ENHANCED DATABASE MANAGER WITH SUPABASE SUPPORT
# =============================================================================

class EnhancedDatabaseManager:
    """Database manager with Supabase and local SQLite support"""

    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.logger = logging.getLogger(__name__)

        # Supabase configuration
        self.supabase_url = config.get('supabase_url', os.environ.get('SUPABASE_URL'))
        self.supabase_key = config.get('supabase_key', os.environ.get('SUPABASE_KEY'))
        self.supabase_client = None

        # Local SQLite fallback
        self.local_db_path = config.get('local_db_path', 'paper_trading.db')
        self.local_conn = None

        # Initialize connections
        self._init_supabase()
        self._init_local_db()

        # Determine primary database
        self.use_supabase = self.supabase_client is not None

        if self.use_supabase:
            self.logger.info("Using Supabase as primary database")
        else:
            self.logger.info("Using local SQLite as primary database")

    def _init_supabase(self):
        """Initialize Supabase connection"""
        if not SUPABASE_AVAILABLE:
            self.logger.warning("Supabase library not available")
            return

        if not self.supabase_url or not self.supabase_key:
            self.logger.warning("Supabase URL or key not configured")
            return

        try:
            self.supabase_client = create_client(self.supabase_url, self.supabase_key)
            # Test connection
            response = self.supabase_client.table('bets').select('count', count='exact').limit(1).execute()
            self.logger.info(f"Supabase connected successfully. Total bets: {response.count}")
        except Exception as e:
            self.logger.error(f"Failed to connect to Supabase: {e}")
            self.supabase_client = None

    def _init_local_db(self):
        """Initialize local SQLite database"""
        try:
            self.local_conn = sqlite3.connect(self.local_db_path, check_same_thread=False)
            self.local_conn.row_factory = sqlite3.Row
            self._create_tables()
            self.logger.info(f"Local SQLite database initialized: {self.local_db_path}")
        except Exception as e:
            self.logger.error(f"Failed to initialize local database: {e}")
            self.local_conn = None

    def _create_tables(self):
        """Create necessary tables in local database"""
        cursor = self.local_conn.cursor()

        # Bets table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bets (
                id TEXT PRIMARY KEY,
                sport TEXT,
                event TEXT,
                market TEXT,
                player TEXT,
                selection TEXT,
                odds REAL,
                stake REAL,
                result TEXT,
                profit REAL,
                status TEXT DEFAULT 'pending',
                resolution_state TEXT DEFAULT 'pending',
                resolution_error TEXT,
                last_resolution_attempt TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                game_date DATE,
                source TEXT,
                raw_data TEXT
            )
        ''')

        # Results cache table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS results_cache (
                cache_key TEXT PRIMARY KEY,
                player_name TEXT,
                sport TEXT,
                game_date DATE,
                stats_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP
            )
        ''')

        # Audit trail table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS audit_trail (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bet_id TEXT,
                action TEXT,
                details TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Create indexes
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_bets_status ON bets(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_bets_player ON bets(player)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_bets_game_date ON bets(game_date)')
        cursor.execute(
            'CREATE INDEX IF NOT EXISTS idx_results_cache_player ON results_cache(player_name, sport, game_date)')

        self.local_conn.commit()

    def add_bet(self, bet_data: Dict) -> str:
        """Add a new bet to the database"""
        bet_id = bet_data.get('id') or str(uuid.uuid4())

        # Prepare bet record
        bet_record = {
            'id': bet_id,
            'sport': bet_data.get('sport', '').lower(),
            'event': bet_data.get('event', ''),
            'market': bet_data.get('market', ''),
            'player': bet_data.get('player', ''),
            'selection': bet_data.get('selection', ''),
            'odds': float(bet_data.get('odds', 0)),
            'stake': float(bet_data.get('stake', 0)),
            'result': bet_data.get('result'),
            'profit': bet_data.get('profit'),
            'status': bet_data.get('status', 'pending'),
            'resolution_state': bet_data.get('resolution_state', 'pending'),
            'game_date': bet_data.get('game_date'),
            'source': bet_data.get('source', 'manual'),
            'raw_data': json.dumps(bet_data) if bet_data.get('raw_data') else None,
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }

        try:
            if self.use_supabase:
                # Insert into Supabase
                response = self.supabase_client.table('bets').insert(bet_record).execute()
                self.logger.info(f"Bet added to Supabase: {bet_id}")
            else:
                # Insert into local SQLite
                cursor = self.local_conn.cursor()
                columns = ', '.join(bet_record.keys())
                placeholders = ', '.join(['?'] * len(bet_record))
                sql = f"INSERT OR REPLACE INTO bets ({columns}) VALUES ({placeholders})"
                cursor.execute(sql, list(bet_record.values()))
                self.local_conn.commit()
                self.logger.info(f"Bet added to local database: {bet_id}")

            # Add to audit trail
            self._add_audit_trail(bet_id, 'bet_added',
                                  f"New bet added: {bet_record['player']} - {bet_record['market']}")

            return bet_id

        except Exception as e:
            self.logger.error(f"Failed to add bet: {e}")
            raise

    def get_bet(self, bet_id: str) -> Optional[Dict]:
        """Get a bet by ID"""
        try:
            if self.use_supabase:
                response = self.supabase_client.table('bets').select('*').eq('id', bet_id).execute()
                if response.data:
                    return response.data[0]
            else:
                cursor = self.local_conn.cursor()
                cursor.execute("SELECT * FROM bets WHERE id = ?", (bet_id,))
                row = cursor.fetchone()
                if row:
                    return dict(row)
        except Exception as e:
            self.logger.error(f"Failed to get bet {bet_id}: {e}")

        return None

    def get_pending_bets(self, limit: int = 100) -> List[Dict]:
        """Get all pending bets"""
        try:
            if self.use_supabase:
                response = self.supabase_client.table('bets') \
                    .select('*') \
                    .or_('status.is.null,status.eq.pending') \
                    .order('created_at', desc=False) \
                    .limit(limit) \
                    .execute()
                return response.data
            else:
                cursor = self.local_conn.cursor()
                cursor.execute('''
                    SELECT * FROM bets 
                    WHERE status IS NULL OR status = 'pending'
                    ORDER BY created_at ASC
                    LIMIT ?
                ''', (limit,))
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            self.logger.error(f"Failed to get pending bets: {e}")
            return []

    def update_bet_result(self, bet_id: str, won: bool, profit: float):
        """Update bet result"""
        try:
            result = 'win' if won else 'loss'
            status = 'resolved'

            update_data = {
                'result': result,
                'profit': profit,
                'status': status,
                'resolution_state': 'complete',
                'updated_at': datetime.now().isoformat()
            }

            if self.use_supabase:
                response = self.supabase_client.table('bets') \
                    .update(update_data) \
                    .eq('id', bet_id) \
                    .execute()
            else:
                cursor = self.local_conn.cursor()
                set_clause = ', '.join([f"{key} = ?" for key in update_data.keys()])
                values = list(update_data.values()) + [bet_id]
                cursor.execute(f"UPDATE bets SET {set_clause} WHERE id = ?", values)
                self.local_conn.commit()

            # Add to audit trail
            self._add_audit_trail(bet_id, 'bet_resolved', f"Bet resolved: {result} (profit: {profit})")

            self.logger.info(f"Bet {bet_id} updated: {result}, profit: {profit}")

        except Exception as e:
            self.logger.error(f"Failed to update bet result: {e}")
            raise

    def update_bet_error(self, bet_id: str, error: str):
        """Update bet with error"""
        try:
            update_data = {
                'resolution_error': error,
                'updated_at': datetime.now().isoformat(),
                'last_resolution_attempt': datetime.now().isoformat()
            }

            if self.use_supabase:
                response = self.supabase_client.table('bets') \
                    .update(update_data) \
                    .eq('id', bet_id) \
                    .execute()
            else:
                cursor = self.local_conn.cursor()
                set_clause = ', '.join([f"{key} = ?" for key in update_data.keys()])
                values = list(update_data.values()) + [bet_id]
                cursor.execute(f"UPDATE bets SET {set_clause} WHERE id = ?", values)
                self.local_conn.commit()

            # Add to audit trail
            self._add_audit_trail(bet_id, 'resolution_error', f"Resolution error: {error}")

        except Exception as e:
            self.logger.error(f"Failed to update bet error: {e}")

    def update_bet_state(self, bet_id: str, state: str, note: str = ""):
        """Update bet resolution state"""
        try:
            update_data = {
                'resolution_state': state,
                'updated_at': datetime.now().isoformat(),
                'last_resolution_attempt': datetime.now().isoformat()
            }

            if self.use_supabase:
                response = self.supabase_client.table('bets') \
                    .update(update_data) \
                    .eq('id', bet_id) \
                    .execute()
            else:
                cursor = self.local_conn.cursor()
                set_clause = ', '.join([f"{key} = ?" for key in update_data.keys()])
                values = list(update_data.values()) + [bet_id]
                cursor.execute(f"UPDATE bets SET {set_clause} WHERE id = ?", values)
                self.local_conn.commit()

            # Add to audit trail
            self._add_audit_trail(bet_id, 'state_change', f"State changed to {state}: {note}")

        except Exception as e:
            self.logger.error(f"Failed to update bet state: {e}")

    def _add_audit_trail(self, bet_id: str, action: str, details: str):
        """Add entry to audit trail"""
        try:
            audit_record = {
                'bet_id': bet_id,
                'action': action,
                'details': details,
                'timestamp': datetime.now().isoformat()
            }

            if self.use_supabase:
                if hasattr(self.supabase_client.table, 'audit_trail'):
                    self.supabase_client.table('audit_trail').insert(audit_record).execute()
            else:
                cursor = self.local_conn.cursor()
                cursor.execute(
                    "INSERT INTO audit_trail (bet_id, action, details) VALUES (?, ?, ?)",
                    (bet_id, action, details)
                )
                self.local_conn.commit()
        except Exception as e:
            self.logger.debug(f"Failed to add audit trail: {e}")

    def get_bet_stats(self) -> Dict:
        """Get betting statistics"""
        try:
            if self.use_supabase:
                # Get total bets
                total_response = self.supabase_client.table('bets').select('count', count='exact').execute()
                total_bets = total_response.count or 0

                # Get resolved bets
                resolved_response = self.supabase_client.table('bets') \
                    .select('*', count='exact') \
                    .eq('status', 'resolved') \
                    .execute()
                resolved_bets = resolved_response.count or 0

                # Get win/loss stats
                wins_response = self.supabase_client.table('bets') \
                    .select('profit', count='exact') \
                    .eq('result', 'win') \
                    .eq('status', 'resolved') \
                    .execute()
                wins = wins_response.count or 0

                # Calculate total profit
                profit_response = self.supabase_client.table('bets') \
                    .select('profit') \
                    .eq('status', 'resolved') \
                    .execute()
                total_profit = sum(bet['profit'] for bet in profit_response.data) if profit_response.data else 0

            else:
                cursor = self.local_conn.cursor()

                # Total bets
                cursor.execute("SELECT COUNT(*) FROM bets")
                total_bets = cursor.fetchone()[0]

                # Resolved bets
                cursor.execute("SELECT COUNT(*) FROM bets WHERE status = 'resolved'")
                resolved_bets = cursor.fetchone()[0]

                # Wins
                cursor.execute("SELECT COUNT(*) FROM bets WHERE result = 'win' AND status = 'resolved'")
                wins = cursor.fetchone()[0]

                # Total profit
                cursor.execute("SELECT SUM(profit) FROM bets WHERE status = 'resolved'")
                total_profit = cursor.fetchone()[0] or 0

            losses = resolved_bets - wins
            win_rate = (wins / resolved_bets * 100) if resolved_bets > 0 else 0

            return {
                'total_bets': total_bets,
                'resolved_bets': resolved_bets,
                'wins': wins,
                'losses': losses,
                'win_rate': win_rate,
                'total_profit': total_profit,
                'avg_profit': total_profit / resolved_bets if resolved_bets > 0 else 0
            }

        except Exception as e:
            self.logger.error(f"Failed to get bet stats: {e}")
            return {}

    def close(self):
        """Close database connections"""
        if self.local_conn:
            self.local_conn.close()
            self.local_conn = None


# =============================================================================
# ENHANCED SUPABASE SYNC MANAGER
# =============================================================================

class SupabaseSyncManager:
    """Manages synchronization between local SQLite and Supabase"""

    def __init__(self, local_db_manager: EnhancedDatabaseManager):
        self.local_db = local_db_manager
        self.supabase = local_db_manager.supabase_client
        self.logger = logging.getLogger(__name__)

        # Sync tracking
        self.last_sync_time = None
        self.sync_interval_hours = 1  # Sync every hour

    def sync_to_supabase(self):
        """Sync local changes to Supabase"""
        if not self.supabase:
            self.logger.warning("Supabase not available for sync")
            return False

        try:
            # Get local bets that need syncing
            cursor = self.local_db.local_conn.cursor()
            cursor.execute('''
                SELECT * FROM bets 
                WHERE (supabase_synced IS NULL OR supabase_synced = 0)
                OR updated_at > COALESCE(last_sync, '2000-01-01')
            ''')

            local_bets = [dict(row) for row in cursor.fetchall()]

            if not local_bets:
                self.logger.debug("No local bets need syncing")
                return True

            self.logger.info(f"Syncing {len(local_bets)} bets to Supabase")

            # Sync each bet
            for bet in local_bets:
                try:
                    # Remove local-only fields
                    supabase_bet = {k: v for k, v in bet.items() if not k.startswith('local_')}

                    # Insert or update in Supabase
                    response = self.supabase.table('bets').upsert(supabase_bet).execute()

                    # Mark as synced in local database
                    cursor.execute(
                        "UPDATE bets SET supabase_synced = 1, last_sync = ? WHERE id = ?",
                        (datetime.now().isoformat(), bet['id'])
                    )

                except Exception as e:
                    self.logger.error(f"Failed to sync bet {bet['id']}: {e}")

            self.local_db.local_conn.commit()
            self.last_sync_time = datetime.now()
            self.logger.info(f"Sync completed: {len(local_bets)} bets synced")
            return True

        except Exception as e:
            self.logger.error(f"Sync failed: {e}")
            return False

    def sync_from_supabase(self):
        """Sync changes from Supabase to local"""
        if not self.supabase:
            self.logger.warning("Supabase not available for sync")
            return False

        try:
            # Get recent bets from Supabase
            response = self.supabase.table('bets') \
                .select('*') \
                .order('updated_at', desc=True) \
                .limit(100) \
                .execute()

            supabase_bets = response.data

            if not supabase_bets:
                self.logger.debug("No bets to sync from Supabase")
                return True

            self.logger.info(f"Syncing {len(supabase_bets)} bets from Supabase")

            # Sync each bet to local
            cursor = self.local_db.local_conn.cursor()
            for bet in supabase_bets:
                try:
                    # Check if bet exists locally
                    cursor.execute("SELECT id FROM bets WHERE id = ?", (bet['id'],))
                    exists = cursor.fetchone() is not None

                    if exists:
                        # Update existing bet
                        set_clause = ', '.join([f"{key} = ?" for key in bet.keys() if key != 'id'])
                        values = [bet[key] for key in bet.keys() if key != 'id'] + [bet['id']]
                        cursor.execute(f"UPDATE bets SET {set_clause} WHERE id = ?", values)
                    else:
                        # Insert new bet
                        columns = ', '.join(bet.keys())
                        placeholders = ', '.join(['?'] * len(bet))
                        values = list(bet.values())
                        cursor.execute(f"INSERT INTO bets ({columns}) VALUES ({placeholders})", values)

                except Exception as e:
                    self.logger.error(f"Failed to sync bet {bet.get('id')} from Supabase: {e}")

            self.local_db.local_conn.commit()
            self.last_sync_time = datetime.now()
            self.logger.info(f"Sync from Supabase completed: {len(supabase_bets)} bets")
            return True

        except Exception as e:
            self.logger.error(f"Sync from Supabase failed: {e}")
            return False

    def should_sync(self) -> bool:
        """Check if it's time to sync"""
        if not self.last_sync_time:
            return True

        hours_since_sync = (datetime.now() - self.last_sync_time).total_seconds() / 3600
        return hours_since_sync >= self.sync_interval_hours


# =============================================================================
# MAIN PAPER TRADING SYSTEM
# =============================================================================

class PaperTradingSystem:
    """Main paper trading system with all components integrated"""

    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.logger = logging.getLogger(__name__)

        # Setup logging
        self._setup_logging()

        # Initialize components
        self.logger.info("Initializing Paper Trading System...")

        # Database manager
        self.db = EnhancedDatabaseManager(config)

        # R Stats Resolver
        self.r_resolver = RStatsResolver(config)

        # Resolution manager
        self.resolution_manager = IdempotentResolutionManager(self.db, self.r_resolver)

        # Parsers and matchers
        self.bet_parser = UniversalBetParser()
        self.game_matcher = EnhancedGameMatcher()

        # Sync manager (if Supabase is available)
        if self.db.use_supabase:
            self.sync_manager = SupabaseSyncManager(self.db)
        else:
            self.sync_manager = None

        # System state
        self.is_running = False
        self.last_heartbeat = None

        self.logger.info("Paper Trading System initialized successfully")

    def _setup_logging(self):
        """Setup comprehensive logging"""
        log_level = self.config.get('log_level', 'INFO').upper()

        # Configure root logger
        logging.basicConfig(
            level=getattr(logging, log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler('paper_trading.log', encoding='utf-8')
            ]
        )

        # Set specific loggers
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('selenium').setLevel(logging.WARNING)

    def add_bet(self, bet_data: Dict) -> str:
        """Add a new bet to the system"""
        try:
            # Validate required fields
            required_fields = ['sport', 'event', 'market', 'player', 'odds', 'stake']
            for field in required_fields:
                if field not in bet_data or not bet_data[field]:
                    raise ValueError(f"Missing required field: {field}")

            # Parse the bet to extract additional info
            parsed = self.bet_parser.parse_any_bet(
                bet_data['sport'],
                bet_data['event'],
                bet_data['market'],
                bet_data.get('selection', bet_data.get('player', ''))
            )

            # Merge parsed data with bet data
            bet_data.update({
                'parsed_info': json.dumps(parsed),
                'bet_type': parsed['bet_type'],
                'resolution_logic': parsed['resolution_logic']
            })

            # Add to database
            bet_id = self.db.add_bet(bet_data)

            self.logger.info(f"Bet added: {bet_id} - {bet_data['player']} ({bet_data['sport']})")

            return bet_id

        except Exception as e:
            self.logger.error(f"Failed to add bet: {e}")
            raise

    def resolve_bet(self, bet_id: str, force: bool = False) -> Dict:
        """Resolve a specific bet"""
        try:
            self.logger.info(f"Resolving bet {bet_id} (force: {force})")
            result = self.resolution_manager.resolve_bet_safe(bet_id, force)

            # Sync if using Supabase
            if self.sync_manager:
                self.sync_manager.sync_to_supabase()

            return result

        except Exception as e:
            self.logger.error(f"Failed to resolve bet {bet_id}: {e}")
            return {
                'state': 'failed_permanent',
                'error': str(e),
                'bet_id': bet_id
            }

    def resolve_pending_bets(self, max_bets: int = 10, force: bool = False) -> Dict:
        """Resolve all pending bets"""
        try:
            self.logger.info(f"Resolving up to {max_bets} pending bets")
            results = self.resolution_manager.batch_resolve(max_bets, force)

            # Sync if using Supabase
            if self.sync_manager:
                self.sync_manager.sync_to_supabase()

            self.logger.info(f"Batch resolution complete: {results['complete']} resolved, "
                             f"{results['failed_permanent']} failed, "
                             f"{results['retry_pending']} pending retry")

            return results

        except Exception as e:
            self.logger.error(f"Batch resolution failed: {e}")
            return {
                'error': str(e),
                'complete': 0,
                'failed_permanent': 0,
                'retry_pending': 0
            }

    def get_bet_status(self, bet_id: str) -> Dict:
        """Get detailed status of a bet"""
        try:
            bet = self.db.get_bet(bet_id)
            if not bet:
                return {'error': f"Bet {bet_id} not found"}  # Fixed: use double quotes

            resolution_state = self.resolution_manager.STATES.get(
                bet.get('resolution_state', 'PENDING'),
                bet.get('resolution_state', 'pending')
            )

            result = {
                'bet_id': bet_id,
                'player': bet.get('player'),
                'event': bet.get('event'),
                'market': bet.get('market'),
                'status': bet.get('status', 'pending'),
                'resolution_state': resolution_state,
                'result': bet.get('result'),
                'profit': bet.get('profit'),
                'last_resolution_attempt': bet.get('last_resolution_attempt'),
                'resolution_error': bet.get('resolution_error'),
                'created_at': bet.get('created_at')
            }

            # Add resolution manager state if pending
            if resolution_state in ['pending', 'retry_pending', 'resolving']:
                resolver_state = self.r_resolver.get_resolution_state(bet)
                result['resolution_details'] = resolver_state

            return result

        except Exception as e:
            self.logger.error(f"Failed to get bet status: {e}")
            return {'error': str(e)}
    def get_system_stats(self) -> Dict:
        """Get comprehensive system statistics"""
        try:
            # Bet statistics
            bet_stats = self.db.get_bet_stats()

            # R Resolver statistics
            resolver_stats = self.r_resolver.get_performance_stats()

            # System info
            system_info = {
                'database': 'supabase' if self.db.use_supabase else 'sqlite',
                'r_script_available': os.path.exists('bet_resolver.R'),
                'rscript_path': self.r_resolver.rscript_path,
                'sync_available': self.sync_manager is not None,
                'last_heartbeat': self.last_heartbeat.isoformat() if self.last_heartbeat else None
            }

            return {
                'bet_statistics': bet_stats,
                'resolver_performance': resolver_stats,
                'system_info': system_info,
                'timestamp': datetime.now().isoformat()
            }

        except Exception as e:
            self.logger.error(f"Failed to get system stats: {e}")
            return {'error': str(e)}

    def sync_databases(self):
        """Sync between local and Supabase databases"""
        if not self.sync_manager:
            return {'error': 'Sync manager not available'}

        try:
            # Sync in both directions
            to_result = self.sync_manager.sync_to_supabase()
            from_result = self.sync_manager.sync_from_supabase()

            return {
                'sync_to_supabase': 'success' if to_result else 'failed',
                'sync_from_supabase': 'success' if from_result else 'failed',
                'timestamp': datetime.now().isoformat()
            }

        except Exception as e:
            self.logger.error(f"Database sync failed: {e}")
            return {'error': str(e)}

    def test_resolver(self, test_bet: Dict = None) -> Dict:
        """Test the R resolver with a sample bet"""
        try:
            # Use provided bet or create test bet
            if not test_bet:
                test_bet = {
                    'sport': 'nfl',
                    'event': 'Kansas City Chiefs @ San Francisco 49ers',
                    'market': 'Player Passing Yards',
                    'player': 'Patrick Mahomes Over 250.5',
                    'odds': 1.91,
                    'stake': 100.0,
                    'game_date': '2024-02-11'  # Super Bowl date
                }

            self.logger.info(f"Testing resolver with bet: {test_bet['player']}")

            # First test sport normalization
            sport_test_result = self.r_resolver.test_sport_normalization()

            # Try to resolve
            won, profit = self.r_resolver.resolve_player_prop(test_bet)

            return {
                'test_result': 'success',
                'sport_normalization_test': 'passed' if sport_test_result else 'failed',
                'resolution_result': 'win' if won else 'loss',
                'profit': profit,
                'test_bet': test_bet
            }

        except Exception as e:
            self.logger.error(f"Resolver test failed: {e}")
            import traceback
            traceback_str = traceback.format_exc()

            return {
                'test_result': 'failed',
                'error': str(e),
                'traceback': traceback_str,
                'test_bet': test_bet if test_bet else {}
            }

    def run_auto_resolution_cycle(self, interval_minutes: int = 60):
        """Run continuous auto-resolution cycles"""
        self.is_running = True
        self.logger.info(f"Starting auto-resolution cycle (interval: {interval_minutes} minutes)")

        while self.is_running:
            try:
                self.last_heartbeat = datetime.now()

                # Sync databases if needed
                if self.sync_manager and self.sync_manager.should_sync():
                    self.logger.info("Syncing databases...")
                    sync_result = self.sync_databases()
                    self.logger.info(f"Sync result: {sync_result}")

                # Resolve pending bets
                self.logger.info("Checking for pending bets...")
                resolution_result = self.resolve_pending_bets(max_bets=20)

                if resolution_result.get('complete', 0) > 0:
                    self.logger.info(f"Resolved {resolution_result['complete']} bets in this cycle")

                # Clean up old attempts
                self.r_resolver.cleanup_old_attempts()

                # Log system stats periodically
                stats = self.get_system_stats()
                self.logger.info(f"System stats: {stats['bet_statistics'].get('total_bets', 0)} total bets, "
                                 f"{stats['bet_statistics'].get('resolved_bets', 0)} resolved, "
                                 f"{stats['resolver_performance'].get('r_success_rate', 0):.1f}% R success rate")

                # Sleep until next cycle
                self.logger.info(f"Sleeping for {interval_minutes} minutes...")
                for _ in range(interval_minutes * 60):
                    if not self.is_running:
                        break
                    time.sleep(1)

            except KeyboardInterrupt:
                self.logger.info("Auto-resolution stopped by user")
                self.is_running = False
                break

            except Exception as e:
                self.logger.error(f"Error in auto-resolution cycle: {e}")
                time.sleep(60)  # Wait a minute before retrying

    def stop(self):
        """Stop the system"""
        self.is_running = False
        self.db.close()
        self.logger.info("Paper Trading System stopped")


# =============================================================================
# COMMAND LINE INTERFACE
# =============================================================================

class PaperTradingCLI:
    """Command-line interface for the paper trading system"""

    def __init__(self):
        self.system = None
        self.config = self._load_config()

    def _load_config(self) -> Dict:
        """Load configuration from file or environment"""
        config = {
            'log_level': 'INFO',
            'local_db_path': 'paper_trading.db',
            'supabase_url': os.environ.get('SUPABASE_URL'),
            'supabase_key': os.environ.get('SUPABASE_KEY')
        }

        # Try to load from config file
        config_file = 'config.json'
        if os.path.exists(config_file):
            try:
                with open(config_file, 'r') as f:
                    file_config = json.load(f)
                    config.update(file_config)
                    print(f"Loaded configuration from {config_file}")
            except Exception as e:
                print(f"Warning: Could not load config file: {e}")

        return config

    def run(self):
        """Run the CLI"""
        print("\n" + "=" * 80)
        print("ENHANCED SPORTS BETTING PAPER TRADING SYSTEM")
        print("=" * 80)
        print("Features:")
        print("  • R Stats Resolver (nflverse/hoopR)")
        print("  • Idempotent resolution (no double processing)")
        print("  • Supabase + SQLite support")
        print("  • Universal bet parser")
        print("  • Auto-resolution daemon")
        print("=" * 80)

        # Initialize system
        try:
            self.system = PaperTradingSystem(self.config)
        except Exception as e:
            print(f"\n❌ Failed to initialize system: {e}")
            print("\nTroubleshooting tips:")
            print("1. Make sure R is installed and Rscript is in PATH")
            print("2. Check that bet_resolver.R exists in current directory")
            print("3. For Supabase, set SUPABASE_URL and SUPABASE_KEY environment variables")
            return

        # Main menu loop
        while True:
            try:
                self._show_menu()
                choice = input("\nEnter your choice: ").strip()

                if choice == '1':
                    self._add_bet_manual()
                elif choice == '2':
                    self._resolve_bet()
                elif choice == '3':
                    self._resolve_pending()
                elif choice == '4':
                    self._show_bet_status()
                elif choice == '5':
                    self._show_system_stats()
                elif choice == '6':
                    self._test_resolver()
                elif choice == '7':
                    self._run_auto_mode()
                elif choice == '8':
                    self._sync_databases()
                elif choice == '9':
                    self._test_sport_normalization()
                elif choice == '0' or choice.lower() == 'exit':
                    print("\nGoodbye!")
                    if self.system:
                        self.system.stop()
                    break
                else:
                    print("Invalid choice. Please try again.")

            except KeyboardInterrupt:
                print("\n\nOperation cancelled.")
            except Exception as e:
                print(f"\nError: {e}")

    def _show_menu(self):
        """Display main menu"""
        print("\n" + "=" * 80)
        print("MAIN MENU")
        print("=" * 80)
        print("1. Add bet manually")
        print("2. Resolve specific bet")
        print("3. Resolve pending bets")
        print("4. Check bet status")
        print("5. System statistics")
        print("6. Test resolver")
        print("7. Run auto-resolution mode")
        print("8. Sync databases")
        print("9. Test sport normalization")
        print("0. Exit")
        print("=" * 80)

    def _add_bet_manual(self):
        """Add a bet manually via CLI"""
        print("\n" + "=" * 80)
        print("ADD BET MANUALLY")
        print("=" * 80)

        bet_data = {}

        # Get bet details
        bet_data['sport'] = input("Sport (nfl, nba, etc): ").strip()
        bet_data['event'] = input("Event (e.g., 'Lakers vs Celtics'): ").strip()
        bet_data['market'] = input("Market (e.g., 'Player Points'): ").strip()
        bet_data['player'] = input("Player (e.g., 'LeBron James Over 25.5'): ").strip()

        # Get line if not in player name
        if 'over' not in bet_data['player'].lower() and 'under' not in bet_data['player'].lower():
            line = input("Line value (e.g., 25.5, press Enter if none): ").strip()
            if line:
                direction = input("Direction (over/under): ").strip().lower()
                bet_data['player'] = f"{bet_data['player']} {direction} {line}"

        bet_data['odds'] = float(input("Odds (e.g., 1.91): ").strip())
        bet_data['stake'] = float(input("Stake (e.g., 100): ").strip())

        # Optional fields
        game_date = input("Game date (YYYY-MM-DD, optional): ").strip()
        if game_date:
            bet_data['game_date'] = game_date

        source = input("Source (manual, scraper, etc, optional): ").strip()
        if source:
            bet_data['source'] = source

        # Confirm
        print("\nBet Summary:")
        print(f"  Sport: {bet_data['sport']}")
        print(f"  Event: {bet_data['event']}")
        print(f"  Market: {bet_data['market']}")
        print(f"  Player: {bet_data['player']}")
        print(f"  Odds: {bet_data['odds']}")
        print(f"  Stake: {bet_data['stake']}")
        if game_date:
            print(f"  Game Date: {game_date}")

        confirm = input("\nAdd this bet? (y/n): ").strip().lower()
        if confirm == 'y':
            try:
                bet_id = self.system.add_bet(bet_data)
                print(f"\n✅ Bet added successfully! Bet ID: {bet_id}")
            except Exception as e:
                print(f"\n❌ Failed to add bet: {e}")
        else:
            print("\nBet cancelled.")

    def _resolve_bet(self):
        """Resolve a specific bet"""
        print("\n" + "=" * 80)
        print("RESOLVE SPECIFIC BET")
        print("=" * 80)

        bet_id = input("Enter bet ID: ").strip()
        force = input("Force resolution? (y/n): ").strip().lower() == 'y'

        try:
            print(f"\nResolving bet {bet_id}...")
            result = self.system.resolve_bet(bet_id, force)

            print("\nResolution Result:")
            print(f"  State: {result['state']}")

            if result['state'] == 'complete':
                print(f"  Result: {result['result']}")
                print(f"  Profit: €{result['profit']:.2f}")
                print(f"  Resolution ID: {result['resolution_id']}")
            elif result['state'] == 'retry_pending':
                print(f"  Error: {result.get('error', 'Unknown error')}")
                print(f"  Next retry: {result.get('next_retry', 'Unknown')}")
            elif result['state'] == 'failed_permanent':
                print(f"  Error: {result.get('error', 'Unknown error')}")

        except Exception as e:
            print(f"\n❌ Failed to resolve bet: {e}")

    def _resolve_pending(self):
        """Resolve pending bets"""
        print("\n" + "=" * 80)
        print("RESOLVE PENDING BETS")
        print("=" * 80)

        try:
            max_bets = input("Max bets to resolve (default 10): ").strip()
            max_bets = int(max_bets) if max_bets else 10

            force = input("Force resolution? (y/n): ").strip().lower() == 'y'

            print(f"\nResolving up to {max_bets} pending bets...")
            result = self.system.resolve_pending_bets(max_bets, force)

            print("\nBatch Resolution Results:")
            print(f"  Total processed: {result['total']}")
            print(f"  Successfully resolved: {result['complete']}")
            print(f"  Failed permanently: {result['failed_permanent']}")
            print(f"  Pending retry: {result['retry_pending']}")
            print(f"  Manager errors: {result['manager_errors']}")
            print(f"  Batch ID: {result['batch_id']}")

        except Exception as e:
            print(f"\n❌ Failed to resolve pending bets: {e}")

    def _show_bet_status(self):
        """Show status of a bet"""
        print("\n" + "=" * 80)
        print("BET STATUS")
        print("=" * 80)

        bet_id = input("Enter bet ID: ").strip()

        try:
            status = self.system.get_bet_status(bet_id)

            if 'error' in status:
                print(f"\n❌ Error: {status['error']}")
                return

            print("\nBet Status:")
            print(f"  Bet ID: {status['bet_id']}")
            print(f"  Player: {status['player']}")
            print(f"  Event: {status['event']}")
            print(f"  Market: {status['market']}")
            print(f"  Status: {status['status']}")
            print(f"  Resolution State: {status['resolution_state']}")

            if status['result']:
                print(f"  Result: {status['result']}")
                print(f"  Profit: €{status.get('profit', 0):.2f}")

            if status['resolution_error']:
                print(f"  Last Error: {status['resolution_error']}")

            if status['last_resolution_attempt']:
                print(f"  Last Attempt: {status['last_resolution_attempt']}")

            print(f"  Created: {status['created_at']}")

            if 'resolution_details' in status:
                print("\nResolution Details:")
                for key, value in status['resolution_details'].items():
                    print(f"  {key}: {value}")

        except Exception as e:
            print(f"\n❌ Failed to get bet status: {e}")

    def _show_system_stats(self):
        """Show system statistics"""
        print("\n" + "=" * 80)
        print("SYSTEM STATISTICS")
        print("=" * 80)

        try:
            stats = self.system.get_system_stats()

            if 'error' in stats:
                print(f"\n❌ Error: {stats['error']}")
                return

            # Bet statistics
            bet_stats = stats['bet_statistics']
            print("\n📊 BET STATISTICS:")
            print(f"  Total bets: {bet_stats.get('total_bets', 0)}")
            print(f"  Resolved bets: {bet_stats.get('resolved_bets', 0)}")
            print(f"  Wins: {bet_stats.get('wins', 0)}")
            print(f"  Losses: {bet_stats.get('losses', 0)}")
            print(f"  Win rate: {bet_stats.get('win_rate', 0):.1f}%")
            print(f"  Total profit: €{bet_stats.get('total_profit', 0):.2f}")
            print(f"  Average profit: €{bet_stats.get('avg_profit', 0):.2f}")

            # Resolver performance
            resolver_stats = stats['resolver_performance']
            print("\n🔧 RESOLVER PERFORMANCE:")
            print(f"  R calls total: {resolver_stats.get('r_calls_total', 0)}")
            print(f"  R calls successful: {resolver_stats.get('r_calls_successful', 0)}")
            print(f"  R success rate: {resolver_stats.get('r_success_rate', 0):.1f}%")
            print(f"  Active resolution attempts: {resolver_stats.get('active_resolution_attempts', 0)}")
            print(f"  Cache size: {resolver_stats.get('cache_size', 0)}")

            # System info
            system_info = stats['system_info']
            print("\n💻 SYSTEM INFO:")
            print(f"  Database: {system_info.get('database', 'unknown')}")
            print(f"  R script available: {system_info.get('r_script_available', False)}")
            print(f"  Rscript path: {system_info.get('rscript_path', 'not found')}")
            print(f"  Sync available: {system_info.get('sync_available', False)}")

            print(f"\n📅 Last updated: {stats['timestamp']}")

        except Exception as e:
            print(f"\n❌ Failed to get system stats: {e}")

    def _test_resolver(self):
        """Test the R resolver"""
        print("\n" + "=" * 80)
        print("TEST RESOLVER")
        print("=" * 80)

        print("\nChoose test option:")
        print("1. Test with default Super Bowl bet")
        print("2. Enter custom test bet")

        choice = input("\nEnter choice: ").strip()

        try:
            if choice == '1':
                # Default test bet
                test_bet = {
                    'sport': 'nfl',
                    'event': 'Kansas City Chiefs @ San Francisco 49ers',
                    'market': 'Player Passing Yards',
                    'player': 'Patrick Mahomes Over 250.5',
                    'odds': 1.91,
                    'stake': 100.0,
                    'game_date': '2024-02-11'
                }

                print(f"\nUsing test bet: {test_bet['player']}")
                result = self.system.test_resolver(test_bet)

            elif choice == '2':
                # Custom test bet
                test_bet = {}
                test_bet['sport'] = input("Sport: ").strip()
                test_bet['event'] = input("Event: ").strip()
                test_bet['market'] = input("Market: ").strip()
                test_bet['player'] = input("Player: ").strip()
                test_bet['odds'] = float(input("Odds: ").strip())
                test_bet['stake'] = float(input("Stake: ").strip())
                test_bet['game_date'] = input("Game date (YYYY-MM-DD): ").strip()

                result = self.system.test_resolver(test_bet)

            else:
                print("Invalid choice")
                return

            print("\nTest Results:")
            print(f"  Test result: {result['test_result']}")

            if result['test_result'] == 'success':
                print(f"  Sport normalization: {result['sport_normalization_test']}")
                print(f"  Resolution result: {result['resolution_result']}")
                print(f"  Profit: €{result['profit']:.2f}")

                print("\nTest bet details:")
                for key, value in result['test_bet'].items():
                    print(f"  {key}: {value}")

            else:
                print(f"  Error: {result['error']}")
                if 'traceback' in result:
                    print("\nTraceback:")
                    print(result['traceback'])

        except Exception as e:
            print(f"\n❌ Test failed: {e}")

    def _run_auto_mode(self):
        """Run auto-resolution mode"""
        print("\n" + "=" * 80)
        print("AUTO-RESOLUTION MODE")
        print("=" * 80)

        interval = input("\nCheck interval in minutes (default 60): ").strip()
        interval = int(interval) if interval else 60

        print(f"\nStarting auto-resolution mode (checking every {interval} minutes)...")
        print("Press Ctrl+C to stop")

        try:
            self.system.run_auto_resolution_cycle(interval)
        except KeyboardInterrupt:
            print("\nAuto-resolution mode stopped.")
        except Exception as e:
            print(f"\n❌ Auto-resolution failed: {e}")

    def _sync_databases(self):
        """Sync databases"""
        print("\n" + "=" * 80)
        print("SYNC DATABASES")
        print("=" * 80)

        try:
            print("\nSyncing databases...")
            result = self.system.sync_databases()

            if 'error' in result:
                print(f"\n❌ Sync failed: {result['error']}")
            else:
                print("\nSync Results:")
                print(f"  To Supabase: {result.get('sync_to_supabase', 'N/A')}")
                print(f"  From Supabase: {result.get('sync_from_supabase', 'N/A')}")
                print(f"  Timestamp: {result.get('timestamp', 'N/A')}")

        except Exception as e:
            print(f"\n❌ Sync failed: {e}")

    def _test_sport_normalization(self):
        """Test sport normalization"""
        print("\n" + "=" * 80)
        print("TEST SPORT NORMALIZATION")
        print("=" * 80)

        try:
            print("\nRunning sport normalization tests...")
            result = self.system.r_resolver.test_sport_normalization()

            if result:
                print("\n✅ All sport normalization tests passed!")
            else:
                print("\n❌ Some sport normalization tests failed")

        except Exception as e:
            print(f"\n❌ Test failed: {e}")


# =============================================================================
# QUICK START EXAMPLES
# =============================================================================

def quick_start():
    """Quick start examples"""
    print("\n" + "=" * 80)
    print("QUICK START EXAMPLES")
    print("=" * 80)

    print("\n1. Initialize the system:")
    print("""
    config = {
        'log_level': 'INFO',
        'local_db_path': 'paper_trading.db'
    }
    system = PaperTradingSystem(config)
    """)

    print("\n2. Add a bet:")
    print("""
    bet_data = {
        'sport': 'nfl',
        'event': 'Chiefs @ 49ers',
        'market': 'Player Passing Yards',
        'player': 'Patrick Mahomes Over 250.5',
        'odds': 1.91,
        'stake': 100.0,
        'game_date': '2024-02-11'
    }
    bet_id = system.add_bet(bet_data)
    """)

    print("\n3. Resolve a bet:")
    print("""
    result = system.resolve_bet(bet_id)
    print(f"Result: {result['result']}, Profit: {result['profit']}")
    """)

    print("\n4. Run auto-resolution:")
    print("""
    # Runs in background, checks every hour
    import threading
    thread = threading.Thread(target=system.run_auto_resolution_cycle, args=(60,))
    thread.start()
    """)

    print("\n5. Get system statistics:")
    print("""
    stats = system.get_system_stats()
    print(f"Total bets: {stats['bet_statistics']['total_bets']}")
    print(f"Win rate: {stats['bet_statistics']['win_rate']:.1f}%")
    """)


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def main():
    """Main entry point"""
    # Parse command line arguments
    import argparse

    parser = argparse.ArgumentParser(description='Sports Betting Paper Trading System')
    parser.add_argument('--cli', action='store_true', help='Run interactive CLI')
    parser.add_argument('--quick-start', action='store_true', help='Show quick start examples')
    parser.add_argument('--test', action='store_true', help='Run resolver test')
    parser.add_argument('--auto', action='store_true', help='Run auto-resolution mode')
    parser.add_argument('--interval', type=int, default=60, help='Auto-resolution interval in minutes')
    parser.add_argument('--resolve-pending', type=int, help='Resolve N pending bets')
    parser.add_argument('--config', type=str, help='Path to config file')

    args = parser.parse_args()

    if args.quick_start:
        quick_start()
        return

    # Load config if specified
    config = {}
    if args.config and os.path.exists(args.config):
        try:
            with open(args.config, 'r') as f:
                config = json.load(f)
        except Exception as e:
            print(f"Warning: Could not load config file: {e}")

    # Run based on arguments
    if args.cli or not any([args.test, args.auto, args.resolve_pending]):
        # Interactive CLI mode
        cli = PaperTradingCLI()
        cli.run()

    elif args.test:
        # Test mode
        print("Running resolver test...")
        system = PaperTradingSystem(config)
        result = system.test_resolver()
        print(f"\nTest result: {result}")
        system.stop()

    elif args.auto:
        # Auto-resolution mode
        print(f"Starting auto-resolution mode (interval: {args.interval} minutes)...")
        system = PaperTradingSystem(config)

        try:
            system.run_auto_resolution_cycle(args.interval)
        except KeyboardInterrupt:
            print("\nAuto-resolution stopped by user")
        finally:
            system.stop()

    elif args.resolve_pending:
        # Resolve pending bets
        print(f"Resolving {args.resolve_pending} pending bets...")
        system = PaperTradingSystem(config)
        result = system.resolve_pending_bets(args.resolve_pending)
        print(f"\nResolution complete: {result}")
        system.stop()


if __name__ == "__main__":
    main()


