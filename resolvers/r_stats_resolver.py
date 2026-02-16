"""
R Stats Resolver - Production-grade bet resolver using R (nflverse/hoopR)
"""
import os
import json
import sqlite3
import hashlib
import subprocess
import platform
import logging
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class RStatsResolver:
    """
    Production-grade bet resolver using R (nflverse/hoopR)
    Python is thin orchestrator, R is source of truth for all game data
    """

    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.is_windows = platform.system() == "Windows"

        # R configuration
        self.rscript_path = self._find_rscript()
        if not self.rscript_path:
            raise RuntimeError("Rscript not found. Install R from https://cran.r-project.org/")

        import os
        # Try multiple possible locations
        possible_paths = [
            os.path.join(os.getcwd(), "bet_resolver.R"),  # Current working directory
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "bet_resolver.R"),
            # Relative from this file
            "/home/runner/work/ArbitrageFinder/bet_resolver.R",  # Absolute GitHub Actions path
            "bet_resolver.R",  # Just the filename
        ]

        self.r_script_path = None
        for path in possible_paths:
            if os.path.exists(path):
                self.r_script_path = os.path.abspath(path)
                self.logger.info(f"Found R script at: {self.r_script_path}")
                break

        if not self.r_script_path or not os.path.exists(self.r_script_path):
            # Try to find it anywhere
            import glob
            found = glob.glob("**/bet_resolver.R", recursive=True)
            if found:
                self.r_script_path = os.path.abspath(found[0])
                self.logger.info(f"Found R script via glob: {self.r_script_path}")
            else:
                # Last resort: current directory
                if os.path.exists("bet_resolver.R"):
                    self.r_script_path = os.path.abspath("bet_resolver.R")
                else:
                    raise FileNotFoundError(f"R script not found. Tried: {possible_paths}. Current dir: {os.getcwd()}")

        # Tracking and state
        self.r_calls = 0
        self.r_successful = 0
        self.resolution_attempts: Dict[str, Dict] = {}
        self.max_attempts_per_bet = 5
        self.retry_cooldown_hours = 12

        # Cache system
        self.cache_db = self._init_cache()

        # Import MarketClassifier
        try:
            from resolvers.market_classifier import MarketClassifier
            self.market_classifier = MarketClassifier()
        except ImportError:
            self.logger.warning("MarketClassifier not found, falling back to original behavior")
            self.market_classifier = None

        self.logger.info(f"RStatsResolver initialized (Rscript: {self.rscript_path})")

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

    def _init_cache(self) -> Optional[sqlite3.Connection]:
        """Initialize SQLite cache"""
        try:
            db_path = "../r_stats_cache.db"
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

    def _detect_sport_correctly(self, event: str, sport_hint: str = None) -> Dict[str, any]:
        """
        Detect the correct sport from event string
        Returns dict with sport, league, and is_college flag
        """
        # College team indicators
        college_indicators = [
            'State$', 'University', 'College', 'FL$', 'AL$', 'GA$', 'TX$',
            'CA$', 'OH$', 'IL$', 'PA$', 'NY$', 'NC$', 'MI$', 'NJ$', 'VA$',
            'WA$', 'AZ$', 'TN$', 'IN$', 'MA$', 'MO$', 'MN$', 'WI$', 'CO$',
            'McNeese', 'East Texas A&M', 'UMBC', 'New Hampshire', 'Bethune-Cookman',
            'Alcorn State', 'Kansas State', 'Oklahoma State', 'TCU', 'North Carolina',
            'Pittsburgh', 'NC State', 'Miami \(FL\)', 'Boise State', 'UNLV',
            'Stephen F. Austin', 'Texas A&M-Corpus Christi', 'Coppin State',
            'South Carolina State', 'Florida Atlantic', 'Gonzaga'
        ]

        # Known NBA teams for validation
        nba_teams = [
            'Hawks', 'Celtics', 'Nets', 'Hornets', 'Bulls', 'Cavaliers', 'Mavericks',
            'Nuggets', 'Pistons', 'Warriors', 'Rockets', 'Pacers', 'Clippers', 'Lakers',
            'Grizzlies', 'Heat', 'Bucks', 'Timberwolves', 'Pelicans', 'Knicks',
            'Thunder', 'Magic', '76ers', 'Suns', 'Trail Blazers', 'Kings', 'Spurs',
            'Raptors', 'Jazz', 'Wizards'
        ]

        # Known NFL teams for validation
        nfl_teams = [
            'Cardinals', 'Falcons', 'Ravens', 'Bills', 'Panthers', 'Bears', 'Bengals',
            'Browns', 'Cowboys', 'Broncos', 'Lions', 'Packers', 'Texans', 'Colts',
            'Jaguars', 'Chiefs', 'Raiders', 'Chargers', 'Rams', 'Dolphins', 'Vikings',
            'Patriots', 'Saints', 'Giants', 'Jets', 'Eagles', 'Steelers', '49ers',
            'Seahawks', 'Buccaneers', 'Titans', 'Commanders', 'Football Team'
        ]

        result = {
            'original_event': event,
            'sport_hint': sport_hint,
            'sport': sport_hint,  # Default to hint
            'league': None,
            'is_college': False,
            'is_nfl': False,
            'is_nba': False,
            'confidence': 0.5
        }

        # Check for college indicators
        for indicator in college_indicators:
            if re.search(indicator, event, re.IGNORECASE):
                result['is_college'] = True
                if sport_hint == 'basketball':
                    result['sport'] = 'college_basketball'
                    result['league'] = 'NCAA'
                elif sport_hint == 'football':
                    result['sport'] = 'college_football'
                    result['league'] = 'NCAA'
                else:
                    result['sport'] = 'college'
                result['confidence'] = 0.9
                self.logger.info(f"  Detected college game: {indicator} in event")
                break

        # If not college, check for NBA teams
        if not result['is_college'] and sport_hint in ['basketball', 'nba']:
            for team in nba_teams:
                if re.search(rf'\b{team}\b', event, re.IGNORECASE):
                    result['is_nba'] = True
                    result['sport'] = 'nba'
                    result['league'] = 'NBA'
                    result['confidence'] = 0.9
                    self.logger.info(f"  Detected NBA team: {team}")
                    break

        # Check for NFL teams - This should run regardless of sport_hint
        for team in nfl_teams:
            if re.search(rf'\b{team}\b', event, re.IGNORECASE):
                result['is_nfl'] = True
                result['sport'] = 'nfl'
                result['league'] = 'NFL'
                result['confidence'] = 0.9
                self.logger.info(f"  Detected NFL team: {team}")
                break

        # If we detected NFL but the hint was something else, override it
        if result['is_nfl']:
            result['sport'] = 'nfl'
            
        # If we detected NBA but the hint was something else, override it
        if result['is_nba'] and not result['is_nfl']:
            result['sport'] = 'nba'

        return result

    def _normalize_event_for_espn(self, event_string: str, sport: str = None) -> str:
        """
        Convert team names in event strings to ESPN format.
        FIXED: Only normalizes NBA team names for NBA games.
        College games, NFL, and NHL games are NOT normalized.
        """
        if not event_string:
            return event_string

        # First, detect the correct sport from the event
        sport_info = self._detect_sport_correctly(event_string, sport)
        
        # Log what we detected
        if sport_info['is_college']:
            self.logger.info(f"  College game detected: {event_string} - skipping normalization")
            return event_string  # Don't normalize college games
            
        if sport_info['is_nfl']:
            self.logger.info(f"  NFL game detected: {event_string} - skipping NBA normalization")
            return event_string  # Don't apply NBA normalization to NFL
            
        # CRITICAL FIX: Only normalize NBA team names for NBA games
        if not sport_info['is_nba']:
            # Not an NBA game, return as-is
            return event_string

        self.logger.info(f"  NBA game detected: {event_string} - applying ESPN normalization")

        # Define ESPN displayName mappings in ORDER OF PRIORITY (longest first)
        # This prevents overlapping replacements
        espn_mappings = [
            # FULL TEAM NAMES (longest first - most specific)
            ("Los Angeles Clippers", "LA Clippers"),
            ("Los Angeles Lakers", "Los Angeles Lakers"),
            ("Golden State Warriors", "Golden State Warriors"),
            ("Philadelphia 76ers", "Philadelphia 76ers"),
            ("New York Knicks", "New York Knicks"),
            ("Boston Celtics", "Boston Celtics"),
            ("Miami Heat", "Miami Heat"),
            ("Chicago Bulls", "Chicago Bulls"),
            ("Houston Rockets", "Houston Rockets"),
            ("Sacramento Kings", "Sacramento Kings"),
            ("Oklahoma City Thunder", "Oklahoma City Thunder"),
            ("New Orleans Pelicans", "New Orleans Pelicans"),
            ("Minnesota Timberwolves", "Minnesota Timberwolves"),
            ("Cleveland Cavaliers", "Cleveland Cavaliers"),
            ("Dallas Mavericks", "Dallas Mavericks"),
            ("Denver Nuggets", "Denver Nuggets"),
            ("Phoenix Suns", "Phoenix Suns"),
            ("Milwaukee Bucks", "Milwaukee Bucks"),
            ("Atlanta Hawks", "Atlanta Hawks"),
            ("Brooklyn Nets", "Brooklyn Nets"),
            ("Charlotte Hornets", "Charlotte Hornets"),
            ("Detroit Pistons", "Detroit Pistons"),
            ("Indiana Pacers", "Indiana Pacers"),
            ("Memphis Grizzlies", "Memphis Grizzlies"),
            ("Orlando Magic", "Orlando Magic"),
            ("Portland Trail Blazers", "Portland Trail Blazers"),
            ("San Antonio Spurs", "San Antonio Spurs"),
            ("Toronto Raptors", "Toronto Raptors"),
            ("Utah Jazz", "Utah Jazz"),
            ("Washington Wizards", "Washington Wizards"),
        ]

        # City-only mappings - apply ONLY if full team name is not already in string
        city_only_mappings = [
            ("Golden State", "Golden State Warriors"),
            ("Philadelphia", "Philadelphia 76ers"),
            ("New York", "New York Knicks"),
            ("Boston", "Boston Celtics"),
            ("Miami", "Miami Heat"),
            ("Chicago", "Chicago Bulls"),
            ("Houston", "Houston Rockets"),
        ]

        normalized_event = event_string
        changes_made = []

        # Track what we've already normalized to prevent overlapping replacements
        normalized_teams = set()

        # First pass: Apply full team name mappings (exact matches only)
        for old_name, espn_name in espn_mappings:
            # Check if this exact string is in the event
            if old_name in normalized_event and espn_name not in normalized_teams:
                # Replace only if not already normalized to something else
                already_normalized = False
                for norm_team in normalized_teams:
                    if old_name in norm_team or espn_name in norm_team:
                        already_normalized = True
                        break

                if not already_normalized:
                    before = normalized_event
                    normalized_event = normalized_event.replace(old_name, espn_name)
                    if before != normalized_event:
                        changes_made.append(f"{old_name} -> {espn_name}")
                        normalized_teams.add(espn_name)

        # Second pass: Apply city-only mappings ONLY if full team name not present
        for city_name, full_team_name in city_only_mappings:
            # Skip if the full team name is already in the string
            if full_team_name in normalized_event:
                continue
            # Skip if city is not in the string
            if city_name not in normalized_event:
                continue
            # Apply the replacement
            before = normalized_event
            normalized_event = normalized_event.replace(city_name, full_team_name)
            if before != normalized_event:
                changes_made.append(f"{city_name} -> {full_team_name}")
                normalized_teams.add(full_team_name)

        # Clean up double spaces that might have been introduced
        normalized_event = re.sub(r'\s+', ' ', normalized_event).strip()

        if changes_made:
            self.logger.info(f"Normalized event string:")
            self.logger.info(f"  Original: '{event_string}'")
            self.logger.info(f"  Normalized: '{normalized_event}'")
            self.logger.info(f"  Changes: {', '.join(changes_made)}")

        return normalized_event

    def resolve_player_stat(self, player_name: str, sport: str, season: int,
                            market_type: str, event_string: str, line_value: Optional[float] = None,
                            game_date: Optional[str] = None, direction: Optional[str] = None,
                            stat_type: Optional[str] = None) -> Dict:
        """
        Resolve player stat markets
        """
        return self._call_r_script(
            player_name=player_name,
            sport=sport,
            season=season,
            market_type=market_type,
            event_string=event_string,
            line_value=line_value,
            game_date=game_date,
            direction=direction,
            stat_type=stat_type
        )

    def resolve_scorer(self, player_name: str, sport: str, season: int,
                       scorer_type: str, event_string: str, game_date: Optional[str] = None) -> Dict:
        """
        Resolve scorer markets
        """
        return self._call_r_script(
            player_name=player_name,
            sport=sport,
            season=season,
            market_type=scorer_type,
            event_string=event_string,
            line_value=None,
            game_date=game_date,
            direction=None,
            stat_type=None
        )

    def resolve_game_market(self, event_string: str, sport: str, season: int,
                            market_type: str, line_value: Optional[float] = None,
                            direction: Optional[str] = None, team: Optional[str] = None,
                            game_date: Optional[str] = None) -> Dict:
        """
        Resolve game markets (totals, spreads, moneylines)
        """
        return self._call_r_script(
            player_name=team or "",
            sport=sport,
            season=season,
            market_type=market_type,
            event_string=event_string,
            line_value=line_value,
            game_date=game_date,
            direction=direction,
            stat_type=None
        )

    def _call_r_script(self, player_name: str, sport: str, season: int,
                       market_type: str, event_string: str, line_value: Optional[float],
                       game_date: Optional[str], direction: Optional[str],
                       stat_type: Optional[str]) -> Dict:
        """Call R script with parameters - FIXED for Windows argument parsing"""
        self.logger.info(
            f"ENTERING _call_r_script: player={player_name}, sport={sport}, game_date={game_date}, event={event_string}")

        # First, detect the correct sport from the event
        sport_info = self._detect_sport_correctly(event_string, sport)
        self.logger.info(f"Sport detection result: {sport_info}")
        
        # If it's a college game, return early with a specific message
        if sport_info['is_college']:
            self.logger.info(f"🎓 College game detected, skipping: {event_string}")
            return {
                'success': False,
                'resolved': False,
                'error': f"College game not supported: {event_string}",
                'is_college': True,
                'league': 'NCAA'
            }
            
        # If it's an NFL game, make sure we're using the correct sport
        if sport_info['is_nfl']:
            self.logger.info(f"🏈 NFL game detected: {event_string}")
            # Use NFL sport
            r_sport = 'nfl'
        elif sport_info['is_nba']:
            self.logger.info(f"🏀 NBA game detected: {event_string}")
            r_sport = 'nba'
        else:
            # Fall back to the original sport hint
            r_sport = sport
            self.logger.info(f"❓ Unknown league, using hint: {r_sport}")
            
        # Check if game is in the future
        if game_date:
            try:
                game_date_obj = datetime.strptime(game_date, '%Y-%m-%d').date()
                today = datetime.now().date()
                if game_date_obj > today:
                    self.logger.info(f"⏸ Future game detected: {game_date}")
                    return {
                        'success': False,
                        'resolved': False,
                        'error': f"Game is in the future: {game_date}",
                        'future_game': True,
                        'game_date': game_date
                    }
            except:
                pass

        # NORMALIZE EVENT STRING FOR ESPN (only for NBA)
        normalized_event = event_string
        if r_sport == 'nba':
            normalized_event = self._normalize_event_for_espn(event_string, r_sport)
        else:
            self.logger.info(f"  Not normalizing event for sport: {r_sport}")

        # Use MarketClassifier to get the FULL market name for R
        if self.market_classifier and market_type:
            try:
                # Classify the market
                classification = self.market_classifier.classify(market_type, player_name)

                # Get the full market name for R to parse
                market_for_r = self.market_classifier.get_market_for_r_script(classification)

                self.logger.info(f"DEBUG: Original market_type: '{market_type}'")
                self.logger.info(f"DEBUG: Classified as: {classification.category}.{classification.subcategory}")
                self.logger.info(f"DEBUG: Market for R: '{market_for_r}'")

                # Update market_type to the full market name for R
                market_type = market_for_r
            except Exception as e:
                self.logger.warning(f"Failed to classify market: {e}, using original market_type")

        # Create cache key - FIXED: Handle None values
        cache_parts = {
            "player": (player_name or "").lower().strip(),
            "sport": r_sport,  # Use detected sport
            "season": season,
            "market": (market_type or "").lower().strip(),
            "event": (normalized_event or "").lower().strip(),  # Use normalized event for cache key
            "line": line_value,
            "game_date": game_date or "",
            "direction": direction or "",
            "stat_type": stat_type or ""
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
            # FIX: Build command as shell string with proper quoting for Windows
            # This ensures arguments with spaces are passed correctly

            # Prepare all arguments as strings (USE NORMALIZED EVENT and DETECTED SPORT)
            args_list = [
                player_name or "",
                r_sport,  # Use detected sport
                str(season),
                market_type or "",
                normalized_event or "",  # CRITICAL: Use normalized event here
                str(line_value) if line_value is not None else "NULL",
                game_date if game_date else "NULL",
                direction if direction else "NULL",
                stat_type if stat_type else "NULL"
            ]

            # DEBUG: Log what we're sending
            self.logger.debug(f"Calling R with {len(args_list)} parameters:")
            for i, arg in enumerate(args_list, 1):
                self.logger.debug(f"  Param {i}: '{arg}'")

            # On Windows, we need to use a shell command string with proper quoting
            if self.is_windows:
                # Windows-specific: Build a properly quoted command string
                # Quote each argument to handle spaces and special characters
                quoted_args = []
                for arg in args_list:
                    # Check if argument needs quoting
                    if arg == "NULL":
                        quoted_args.append("NULL")  # NULL doesn't need quotes
                    elif any(c in arg for c in ' \t\n\r+@'):
                        # Contains space, tab, newline, +, @ - needs quoting
                        quoted_args.append(f'"{arg}"')
                    else:
                        quoted_args.append(arg)

                # Build the full command string
                cmd_str = f'"{self.rscript_path}" "{self.r_script_path}" {" ".join(quoted_args)}'

                self.logger.debug(f"Windows command string: {cmd_str[:200]}...")

                # Run with shell=True for command strings
                result = subprocess.run(
                    cmd_str,
                    capture_output=True,
                    text=True,
                    timeout=300,
                    shell=True,  # CRITICAL: shell=True for command strings
                    encoding='utf-8',
                    errors='replace'
                )
            else:
                # Unix/Linux: Use list with shlex.quote
                import shlex
                quoted_args = [shlex.quote(arg) for arg in args_list]
                cmd = [self.rscript_path, self.r_script_path] + quoted_args

                self.logger.debug(f"Unix command list: {' '.join(cmd[:5])}...")

                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=300,
                    shell=False,
                    encoding='utf-8',
                    errors='replace'
                )

            # Log R output for debugging
            if result.stdout:
                self.logger.debug(f"R stdout (first 500 chars): {result.stdout[:500]}")
                # Also log the last 500 chars (where JSON usually is)
                if len(result.stdout) > 500:
                    self.logger.debug(f"R stdout (last 500 chars): {result.stdout[-500:]}")
            if result.stderr:
                self.logger.debug(f"R stderr (first 500 chars): {result.stderr[:500]}")

            # Parse output
            return self._parse_r_output(result.stdout, result.stderr, cache_key)

        except subprocess.TimeoutExpired:
            self.logger.error("R script timeout (300s)")
            return {"success": False, "error": "R script timeout"}
        except Exception as e:
            self.logger.error(f"Error calling R script: {e}", exc_info=True)
            return {"success": False, "error": f"Python error: {str(e)}"}

    def _parse_r_output(self, stdout: str, stderr: str, cache_key: str) -> Dict:
        """Parse R output intelligently"""
        # Combine outputs - FIXED: Handle None values
        all_output = (stdout or "") + (stderr or "")

        if not all_output.strip():
            return {"success": False, "error": "Empty response from R script"}

        # Try to find JSON
        json_line = None
        lines = all_output.strip().split('\n')

        # Search from the end (JSON is usually last)
        for line in reversed(lines):
            line = line.strip()
            if line.startswith('{') and line.endswith('}'):
                json_line = line
                break

        if not json_line:
            # Try more aggressively
            json_match = re.search(r'(\{.*\})', all_output, re.DOTALL)
            if json_match:
                json_line = json_match.group(1)

        # ADD THIS DEBUGGING
        if json_line:
            self.logger.info(f"Found JSON from R: {json_line[:200]}...")
        else:
            self.logger.info(f"No JSON found in R output. Full output: {all_output[:500]}")

        if not json_line:
            # Check for R errors
            if "Fatal error" in all_output or "Error in" in all_output:
                # Extract error message
                error_lines = [line for line in all_output.split('\n')
                               if 'Error' in line or 'Fatal' in line]
                error_msg = error_lines[0] if error_lines else "Unknown R error"
                return {"success": False, "error": error_msg}

            self.logger.debug(f"No JSON found. Full output: {all_output[:1000]}")
            return {
                "success": False,
                "error": f"No JSON found in R output"
            }

        try:
            data = json.loads(json_line)
            self.r_successful += 1

            # Cache successful results
            if data.get("success") and data.get("resolved"):
                self._set_cached_result(cache_key, data, ttl_hours=24)

            return data

        except json.JSONDecodeError as e:
            self.logger.error(f"JSON decode error: {e}")
            self.logger.debug(f"Problematic JSON: {json_line[:500]}")
            return {"success": False, "error": f"Invalid JSON from R: {e}"}

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

    def cleanup_old_attempts(self, max_age_hours: int = 168):
        """Clean up old resolution attempts"""
        cutoff = datetime.now() - timedelta(hours=max_age_hours)
        to_remove = []

        for bet_key, info in self.resolution_attempts.items():
            last_attempt = info.get('last_attempt')
            if last_attempt and last_attempt < cutoff:
                to_remove.append(bet_key)

        for bet_key in to_remove:
            self.resolution_attempts.pop(bet_key, None)

        self.logger.info(f"Cleaned up {len(to_remove)} old resolution attempts")
