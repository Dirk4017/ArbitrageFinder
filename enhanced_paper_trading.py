"""
Enhanced Paper Trading System - Main orchestrator
This replaces your monolithic script with a modular architecture
"""

import sys
import os
import logging
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any

# Add the current directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

# Now import modules
try:
    from resolvers.market_classifier import MarketClassifier, MarketClassification
    from core.config_manager import ConfigManager
    from database.database_factory import create_database_manager
    from scraper.ultra_stable_scanner import UltraStableScanner
    from arbitrage.fixed_arbitrage import FixedArbitrageSystem
    from arbitrage.kelly_bankroll import KellyBankrollManager
    from resolvers.r_stats_resolver import RStatsResolver
    from resolvers.idempotent_resolver import IdempotentResolutionManager
except ImportError as e:
    print(f"Import Error in enhanced_paper_trading.py: {e}")
    print("Current directory:", current_dir)
    print("Python path:", sys.path)
    raise

logger = logging.getLogger(__name__)


class EnhancedPaperTradingSystem:
    """Main orchestrator class - slimmed down version"""

    def __init__(self):
        # Load configuration
        self.config = ConfigManager()

        # Initialize components
        self._init_components()

        # Load state
        self._load_state()

        logger.info("Enhanced Paper Trading System initialized")

    def _init_components(self):
        """Initialize all modular components"""
        # Database
        self.db = create_database_manager(self.config)

        # Market classifier (NEW!)
        self.market_classifier = MarketClassifier()

        # Scanner
        self.scanner = UltraStableScanner()

        # Arbitrage system
        self.arbitrage_system = FixedArbitrageSystem(
            config=self.config,
            bankroll_manager=KellyBankrollManager(self.config)
        )

        # R resolver
        self.r_resolver = RStatsResolver(self.config)

        # Idempotent resolution manager
        self.idempotent_resolver = IdempotentResolutionManager(
            db=self.db,
            r_resolver=self.r_resolver
        )

        # Game matcher (moved to utils/)
        from utils.game_matcher import EnhancedGameMatcher
        self.game_matcher = EnhancedGameMatcher()

    def _load_state(self):
        """Load bankroll state from database"""
        try:
            stats = self.db.get_performance_stats()
            total_profit = stats.get('total_profit', 0)
            if stats['total_bets'] > 0 and total_profit is not None:
                self.bankroll = self.config.bankroll.initial_bankroll + total_profit
                logger.info(f"Loaded bankroll state: €{self.bankroll:.2f}")
            else:
                self.bankroll = self.config.bankroll.initial_bankroll
                logger.info(f"Starting with initial bankroll: €{self.bankroll:.2f}")
        except Exception as e:
            logger.warning(f"Could not load bankroll state: {e}")
            self.bankroll = self.config.bankroll.initial_bankroll

    def _is_future_game(self, game_date: str) -> bool:
        """Check if a game date is in the future"""
        if not game_date:
            return False

        try:
            # Parse the game date
            if ' ' in str(game_date):
                # Has time component
                game_date_obj = datetime.strptime(str(game_date), '%Y-%m-%d %H:%M:%S')
            else:
                # Just date
                game_date_obj = datetime.strptime(str(game_date), '%Y-%m-%d')
                # Assume evening game (8 PM)
                game_date_obj = game_date_obj.replace(hour=20, minute=0, second=0)

            current_time = datetime.now()

            # Add buffer for game completion (3 hours)
            time_buffer = timedelta(hours=3)
            future_cutoff = game_date_obj + time_buffer

            # If current time is before the cutoff, game hasn't completed
            if current_time < future_cutoff:
                return True

            return False

        except Exception as e:
            print(f"Could not parse game date '{game_date}': {e}")
            return True  # Err on side of caution

    def _is_college_game(self, event: str, sport: str) -> bool:
        """Check if this is a college game that shouldn't be resolved"""
        if not event or not sport:
            return False

        event_lower = event.lower()
        sport_lower = sport.lower()

        # If sport is already identified as college, return True
        if sport_lower in ['ncaaf', 'ncaab', 'ncaaw', 'college football', 'college basketball']:
            return True

        # List of NFL team names that contain words that might look like college teams
        nfl_teams = [
            'kansas city chiefs', 'new england patriots', 'new york jets',
            'new york giants', 'los angeles rams', 'los angeles chargers',
            'san francisco 49ers', 'tampa bay buccaneers', 'jacksonville jaguars',
            'carolina panthers', 'arizona cardinals', 'seattle seahawks',
            'buffalo bills', 'miami dolphins', 'pittsburgh steelers',
            'cleveland browns', 'cincinnati bengals', 'baltimore ravens',
            'tennessee titans', 'indianapolis colts', 'houston texans',
            'denver broncos', 'las vegas raiders', 'kansas city chiefs',
            'dallas cowboys', 'philadelphia eagles', 'washington commanders',
            'chicago bears', 'detroit lions', 'green bay packers',
            'minnesota vikings', 'atlanta falcons', 'new orleans saints',
            'carolina panthers'
        ]

        # Check if this is an NFL team first (these are NEVER college)
        for team in nfl_teams:
            if team in event_lower:
                return False

        # College football indicators - but only if we're not sure it's NFL
        if sport_lower in ['football', 'nfl', 'cfb']:
            # First, check for specific college team names
            college_teams = [
                'alabama crimson tide', 'clemson tigers', 'ohio state buckeyes',
                'georgia bulldogs', 'lsu tigers', 'oklahoma sooners',
                'notre dame fighting irish', 'texas longhorns', 'texas a&m aggies',
                'florida gators', 'florida state seminoles', 'miami hurricanes',
                'michigan wolverines', 'usc trojans', 'oregon ducks',
                'penn state nittany lions', 'wisconsin badgers', 'iowa hawkeyes',
                'auburn tigers', 'tennessee volunteers', 'utah utes',
                'baylor bears', 'oklahoma state cowboys', 'kansas state wildcats',
                'kansas jayhawks', 'iowa state cyclones', 'west virginia mountaineers',
                'kentucky wildcats', 'south carolina gamecocks', 'missouri tigers',
                'arkansas razorbacks', 'ole miss rebels', 'mississippi state bulldogs',
                'vanderbilt commodores', 'wake forest demon deacons', 'nc state wolfpack',
                'duke blue devils', 'north carolina tar heels', 'virginia cavaliers',
                'virginia tech hokies', 'clemson tigers', 'georgia tech yellow jackets',
                'florida state seminoles', 'miami hurricanes', 'louisville cardinals',
                'notre dame fighting irish', 'syracuse orange', 'boston college eagles',
                'pittsburgh panthers', 'villanova wildcats', 'marquette golden eagles',
                'creighton bluejays', 'providence friars', 'seton hall pirates',
                'st john\'s red storm', 'georgetown hoyas', 'depaul blue demons',
                'butler bulldogs', 'xavier musketeers', 'uconn huskies',
                'cincinnati bearcats', 'houston cougars', 'byu cougars',
                'ucf knights', 'south florida bulls', 'temple owls',
                'memphis tigers', 'tulsa golden hurricane', 'smu mustangs',
                'tulane green wave', 'east carolina pirates', 'colorado buffaloes',
                'utah utes', 'arizona wildcats', 'arizona state sun devils',
                'ucla bruins', 'usc trojans', 'oregon ducks', 'oregon state beavers',
                'washington huskies', 'washington state cougars', 'california golden bears',
                'stanford cardinal', 'colorado state rams', 'wyoming cowboys',
                'utah state aggies', 'nevada wolf pack', 'unlv rebels',
                'fresno state bulldogs', 'san jose state spartans', 'boise state broncos',
                'air force falcons', 'new mexico lobos', 'san diego state aztecs',
                'st mary\'s gaels', 'san francisco dons', 'pepperdine waves',
                'portland pilots', 'loyola marymount lions', 'pacific tigers',
                'san diego toreros', 'gonzaga bulldogs', 'santa clara broncos',
                'charleston cougars', 'campbell fighting camels', 'mcneese state cowboys',
                'east texas a&m lions', 'bethune-cookman wildcats', 'alcorn state braves',
                'florida atlantic owls', 'florida international panthers', 'charlotte 49ers',
                'north texas mean green', 'rice owls', 'southern miss golden eagles',
                'ul Monroe warhawks', 'ul Lafayette ragin\' cajuns', 'troy trojans',
                'south alabama jaguars', 'georgia southern eagles', 'georgia state panthers',
                'appalachian state mountaineers', 'coastal carolina chanticleers',
                'arkansas state red wolves', 'texas state bobcats', 'louisiana tech bulldogs'
            ]

            for team in college_teams:
                if team in event_lower:
                    return True

            # Also check for standalone college names (but be careful with NFL teams)
            college_keywords = [
                'oregon', 'texas tech', 'clemson', 'alabama', 'georgia',
                'ohio state', 'michigan', 'usc', 'stanford', 'florida state',
                'lsu', 'oklahoma', 'notre dame', 'penn state', 'tennessee',
                'wisconsin', 'iowa', 'michigan state', 'auburn', 'florida',
                'duke', 'north carolina', 'kansas', 'kentucky', 'ucla',
                'gonzaga', 'south carolina', 'uconn', 'louisville', 'baylor',
                'byu', 'cincinnati', 'houston', 'ucf', 'memphis',
                'boise state', 'san diego state', 'colorado state', 'utah state',
                'fresno state', 'unlv', 'wyoming', 'air force', 'new mexico'
            ]

            for keyword in college_keywords:
                # Make sure it's a whole word and not part of an NFL team name
                if f" {keyword} " in f" {event_lower} " or event_lower.startswith(
                        f"{keyword} ") or event_lower.endswith(f" {keyword}"):
                    # Double-check it's not an NFL team
                    is_nfl = False
                    for nfl_team in nfl_teams:
                        if keyword in nfl_team:
                            is_nfl = True
                            break
                    if not is_nfl:
                        return True

        # Check for NCAA/FBS/FCS indicators
        if any(college_marker in event_lower for college_marker in
               ['ncaa', 'fbs', 'fcs', 'cfb', 'pac-12', 'big ten', 'sec', 'acc', 'big 12', 'big east', 'ivy league']):
            # But make sure it's not an NFL game with these terms in the description
            if not any(nfl_team in event_lower for nfl_team in nfl_teams):
                return True

        return False

    def place_intelligent_bet(self, opportunity: Dict) -> bool:
        """Place a bet with intelligent market classification"""
        try:
            # Check for college games
            event = opportunity.get('event', '')
            sport = opportunity.get('sport', 'unknown').lower()

            # ========== DETECT COLLEGE SPORTS FIRST ==========
            # College basketball indicators
            college_basketball_keywords = [
                'tcu', 'oklahoma state', 'kansas state', 'houston',
                'miami (fl)', 'nc state', 'unc', 'north carolina',
                'duke', 'gonzaga', 'santa clara', 'baylor', 'kansas',
                'byu', 'st john\'s', 'providence', 'villanova',
                'umbc', 'new hampshire', 'unlv', 'boise state',
                'iowa state', 'florida atlantic', 'south florida',
                'charleston', 'campbell', 'mcneese state', 'east texas a&m',
                'bethune-cookman', 'alcorn state', 'pittsburgh', 'north carolina',
                'florida atlantic', 'south florida', 'oklahoma state', 'iowa state',
                'gonzaga', 'santa clara', 'st mary\'s', 'san francisco',
                'pepperdine', 'portland', 'loyola marymount', 'pacific',
                'byu', 'san diego', 'kansas state', 'oklahoma state',
                'texas tech', 'west virginia', 'kansas', 'iowa state',
                'baylor', 'tcu', 'oklahoma', 'texas', 'kentucky',
                'florida', 'vanderbilt', 'tennessee', 'south carolina',
                'georgia', 'missouri', 'arkansas', 'lsu', 'auburn',
                'alabama', 'ole miss', 'mississippi state', 'texas a&m',
                'wake forest', 'nc state', 'duke', 'north carolina',
                'virginia', 'virginia tech', 'clemson', 'georgia tech',
                'florida state', 'miami', 'louisville', 'notre dame',
                'syracuse', 'boston college', 'pittsburgh', 'villanova',
                'marquette', 'creighton', 'providence', 'seton hall',
                'st john\'s', 'georgetown', 'depaul', 'butler',
                'xavier', 'uconn', 'cincinnati', 'memphis', 'wichita state',
                'temple', 'smu', 'tulane', 'tulsa', 'east carolina',
                'ucf', 'south florida', 'colorado', 'utah', 'arizona',
                'arizona state', 'ucla', 'usc', 'oregon', 'oregon state',
                'washington', 'washington state', 'california', 'stanford',
                'colorado state', 'wyoming', 'utah state', 'nevada',
                'unlv', 'fresno state', 'san jose state', 'boise state',
                'air force', 'new mexico', 'san diego state',
            ]

            # NBA team names that should NEVER be flagged as college
            nba_team_names = [
                'warriors', 'kings', 'rockets', 'grizzlies', 'lakers', 'clippers',
                'celtics', 'heat', 'bulls', 'knicks', 'mavericks', 'nuggets',
                'suns', 'bucks', '76ers', 'sixers', 'nets', 'raptors', 'hawks',
                'wizards', 'hornets', 'pistons', 'pacers', 'cavaliers', 'magic',
                'timberwolves', 'wolves', 'thunder', 'spurs', 'jazz', 'pelicans'
            ]

            event_lower = event.lower()

            # FIRST: Check if this is clearly an NBA game (should never be college)
            is_nba_game = any(team in event_lower for team in nba_team_names)

            # Check if it's a college basketball game (only if not NBA)
            is_college_basketball = False
            if not is_nba_game:
                is_college_basketball = any(keyword in event_lower for keyword in college_basketball_keywords)

            # Determine the correct sport BEFORE doing anything else
            if is_college_basketball:
                normalized_sport = 'ncaab'
                print(f" COLLEGE BASKETBALL GAME: {event} -> {normalized_sport}")
                logger.info(f" COLLEGE BASKETBALL GAME: {event} -> {normalized_sport}")
            elif is_nba_game:
                normalized_sport = 'nba'
                logger.info(f" NBA GAME DETECTED: {event} -> {normalized_sport}")
            else:
                # Map other professional sports
                sport_mapping = {
                    'basketball': 'nba',  # Default basketball to NBA
                    'football': 'nfl',
                    'hockey': 'nhl',
                    'baseball': 'mlb',
                    'nba': 'nba',
                    'nfl': 'nfl',
                    'nhl': 'nhl',
                    'mlb': 'mlb',
                    'wnba': 'wnba',
                }
                normalized_sport = sport_mapping.get(sport, sport)
                logger.info(f"Professional sport detected: {event} -> {normalized_sport}")

            # ========== FIX: SEASON ADJUSTMENT FOR EARLY 2026 GAMES ==========
            # This is the critical fix for NFL/NBA/NHL games in Jan-Mar 2026
            game_date = opportunity.get('game_date')
            current_date = datetime.now()
            current_year = current_date.year
            current_month = current_date.month

            # Determine the correct season based on sport and date
            if normalized_sport in ['nfl', 'ncaaf']:
                # NFL season: Sep-Feb, so games in Jan/Feb/Mar 2026 belong to 2025 season
                if current_year == 2026 and current_month <= 4:
                    correct_season = 2025
                else:
                    correct_season = current_year
                logger.info(
                    f"🏈 NFL season adjustment: Using {correct_season} (current: {current_year}-{current_month})")
            elif normalized_sport in ['nba', 'nhl', 'ncaab', 'ncaaw']:
                # NBA/NHL season: Oct-Jun, so games in Jan-Jun 2026 belong to 2025 season
                if current_year == 2026 and current_month <= 6:
                    correct_season = 2025
                else:
                    correct_season = current_year
                logger.info(
                    f"🏀/🏒 {normalized_sport.upper()} season adjustment: Using {correct_season} (current: {current_year}-{current_month})")
            else:
                # For other sports, use the existing extraction logic
                correct_season = self._extract_season({'sport': normalized_sport, 'game_date': game_date})

            # CRITICAL: Update the opportunity with the normalized sport and correct season
            opportunity['sport'] = normalized_sport
            opportunity['season'] = correct_season
            # ========== END SEASON FIX ==========

            # 1. Classify the market FIRST
            classification = self.market_classifier.classify(
                market=opportunity['market'],
                player_or_team=opportunity['player']
            )

            logger.info(f"Market classified as: {classification.category}")
            logger.info(f"  Subcategory: {classification.subcategory}")
            logger.info(f"  Stat Type: {classification.stat_type}")
            logger.info(f"  Direction: {classification.direction}, Line: {classification.line_value}")

            # 2. Get Kelly stake based on classification
            pending_bets = self.db.get_pending_bets()
            decimal_odds = self.arbitrage_system.parse_odds(opportunity.get('odds', '1.0'))

            if decimal_odds is None:
                logger.warning(f"Could not parse odds: {opportunity.get('odds')}")
                return False

            # Convert classification to dictionary for stake calculation
            if hasattr(self.market_classifier, 'to_dict'):
                classification_dict = self.market_classifier.to_dict(classification)
            else:
                classification_dict = {
                    'category': classification.category,
                    'subcategory': classification.subcategory,
                    'clean_player': classification.clean_player,
                    'clean_team': classification.clean_team,
                    'direction': classification.direction,
                    'line_value': classification.line_value,
                    'period': classification.period,
                    'stat_type': classification.stat_type,
                    'original_player': classification.original_player,
                    'original_market': classification.original_market,
                    'confidence': classification.confidence
                }

            stake = self.arbitrage_system.calculate_stake(
                ev=opportunity['ev'],
                bankroll=self.bankroll,
                pending_bets=pending_bets,
                odds=decimal_odds,
                market_classification=classification_dict
            )

            if stake <= 0:
                logger.info(f"Stake too low: {stake}")
                return False

            # 3. Prepare bet data with classification info
            bet_data = {
                'event': opportunity['event'],
                'sport': normalized_sport,  # Use normalized sport (ncaab, ncaaf, etc.)
                'market': opportunity['market'],
                'player': opportunity['player'],
                'odds': decimal_odds,
                'stake': stake,
                'potential_win': stake * decimal_odds,
                'ev': opportunity['ev'],
                'sportsbook': opportunity.get('sportsbook', 'Unknown'),
                'game_date': opportunity.get('game_date'),
                'season': correct_season,  # Add the corrected season to the bet data
                'market_category': classification.category,
                'market_subcategory': classification.subcategory,
                'market_stat_type': classification.stat_type,
                'market_line_value': classification.line_value,
                'market_direction': classification.direction,
                'market_period': classification.period
            }

            # 4. Save to database
            bet_id = self.db.save_bet(bet_data)
            if not bet_id:
                logger.error("Failed to save bet to database")
                return False

            # 5. Update bankroll
            self.bankroll -= stake

            logger.info(f" Bet placed: {opportunity['player']} - {opportunity['market']}")
            logger.info(f"  Stake: €{stake:.2f}, Classification: {classification.category}")
            logger.info(
                f"  Sport SAVED AS: {normalized_sport}, Season: {correct_season}, Stat Type: {classification.stat_type}")

            return True

        except Exception as e:
            logger.error(f"Error placing intelligent bet: {e}")
            return False

    def resolve_bet_intelligently(self, bet_id: str) -> Dict:
        """
        Resolve a bet using intelligent market classification
        Routes to appropriate resolver based on market type
        """
        try:
            # Get bet from database
            bet = self.db.get_bet(bet_id)
            if not bet:
                return {'success': False, 'error': 'Bet not found', 'resolved': False}

            # Check for college games
            event = bet.get('event', '')
            sport = bet.get('sport', '')

            if self._is_college_game(event, sport):
                return {
                    'success': True,
                    'resolved': False,
                    'message': f'College game not supported: {event}',
                    'skip_reason': 'college_game'
                }

            # Check if game is in the future
            game_date = bet.get('game_date')
            if game_date and self._is_future_game(game_date):
                try:
                    game_dt = datetime.strptime(str(game_date), '%Y-%m-%d')
                    current_dt = datetime.now()
                    days_until = (game_dt - current_dt).days
                except:
                    days_until = None

                message = f"Game is in the future: {game_date}"
                if days_until is not None:
                    message += f" ({days_until} days from now)"

                return {
                    'success': True,
                    'resolved': False,
                    'message': message,
                    'skip_reason': 'future_game'
                }

            # Classify the market if not already classified
            if 'market_category' not in bet:
                classification = self.market_classifier.classify(
                    market=bet['market'],
                    player_or_team=bet['player']
                )
            else:
                # Use existing classification
                classification = MarketClassification(
                    category=bet.get('market_category', 'player_prop'),
                    subcategory=bet.get('market_subcategory', 'generic'),
                    clean_player=bet.get('player', ''),
                    clean_team='',
                    direction=bet.get('market_direction'),
                    line_value=bet.get('market_line_value'),
                    period=bet.get('market_period'),
                    stat_type=bet.get('market_stat_type'),
                    original_player=bet.get('player', ''),
                    original_market=bet.get('market', '')
                )

            # Route to appropriate resolver based on classification
            resolution_type = self.market_classifier.get_resolution_type(classification)

            # Log the resolution type for debugging
            logger.info(f"Resolving bet {bet_id} as {resolution_type} for sport {sport}")

            if resolution_type.startswith('player_') and resolution_type != 'player_stat':
                # Player stat markets (including MLB, NBA, NFL, etc.)
                return self._resolve_player_stat(bet, classification)
            elif resolution_type in ['first_goal_scorer', 'last_goal_scorer',
                                     'first_touchdown_scorer', 'last_touchdown_scorer',
                                     'first_basket_scorer', 'first_field_goal']:
                return self._resolve_scorer(bet, classification)
            elif resolution_type in ['moneyline', 'point_spread', 'total_points']:
                return self._resolve_game_market(bet, classification)
            else:
                # Fallback to idempotent resolver
                logger.info(f"Using fallback resolver for {resolution_type}")
                return self.idempotent_resolver.resolve_bet_safe(bet_id)

        except Exception as e:
            logger.error(f"Intelligent resolution failed: {e}")
            return {'success': False, 'error': str(e), 'resolved': False}

    def _is_void_error(self, error_msg: str) -> bool:
        """Check if an error message indicates a void bet (player didn't play)"""
        if not error_msg:
            return False
        error_lower = error_msg.lower()
        void_phrases = [
            'no player stats',
            'player not found',
            'no stats',
        ]
        return any(phrase in error_lower for phrase in void_phrases)

    def _void_bet(self, bet: Dict, reason: str = 'player_did_not_play') -> Dict:
        """Void a bet - refund stake and update database"""
        stake = float(bet.get('stake', 0))
        self.db.update_bet_result(bet['id'], None, 0, status='void', void_reason=reason)
        self.bankroll += stake  # Return stake to bankroll
        logger.info(f"Bet {bet['id']} voided: {reason} - Stake returned: €{stake:.2f}")
        return {
            'success': True,
            'resolved': True,
            'won': None,
            'void': True,
            'void_reason': reason,
            'profit': 0,
            'stake_returned': stake,
            'error': None,
            'message': f"VOID - {reason} - Stake returned: €{stake:.2f}",
        }

    def _resolve_player_stat(self, bet: Dict, classification: MarketClassification) -> Dict:
        """Resolve player stat markets for all sports"""

        # ========== FORMAT MARKET TYPE FOR R SCRIPT ==========
        sport = bet.get('sport', '').lower()
        market_type = classification.subcategory
        stat_type = classification.stat_type

        # Format based on sport and stat type
        if sport in ['nba', 'basketball', 'wnba', 'ncaab', 'ncaaw']:
            if stat_type == 'points':
                market_type = "Player Points"
            elif stat_type == 'rebounds':
                market_type = "Player Rebounds"
            elif stat_type == 'assists':
                market_type = "Player Assists"
            elif stat_type == 'three_pointers_made':
                market_type = "threes"
            elif 'points_assists' in classification.subcategory:
                market_type = "Points + Assists"
            elif 'points_rebounds' in classification.subcategory:
                market_type = "Points + Rebounds"
            elif 'rebounds_assists' in classification.subcategory:
                market_type = "Rebounds + Assists"
            elif 'points_rebounds_assists' in classification.subcategory:
                market_type = "Points + Rebounds + Assists"

        elif sport in ['nfl', 'football']:
            if stat_type == 'passing_yards':
                market_type = "Player Passing Yards"
            elif stat_type == 'receiving_yards':
                market_type = "Player Receiving Yards"
            elif stat_type == 'rushing_yards':
                market_type = "Player Rushing Yards"
            elif stat_type == 'receptions':
                market_type = "Player Receptions"
            elif stat_type == 'touchdowns':
                market_type = "Player Touchdowns"

        elif sport in ['mlb', 'baseball']:
            if stat_type == 'hits':
                market_type = "Player Hits"
            elif stat_type == 'home_runs':
                market_type = "Player Home Runs"
            elif stat_type == 'rbi':
                market_type = "Player RBI"

        elif sport in ['nhl', 'hockey']:
            if stat_type == 'goals':
                market_type = "Player Goals"
            elif stat_type == 'assists':
                market_type = "Player Assists"
            elif stat_type == 'shots':
                market_type = "Player Shots On Goal"
            elif stat_type == 'saves':
                market_type = "Player Saves"

        # Prepare parameters for R with formatted market_type
        params = {
            'player_name': classification.clean_player,
            'sport': sport,
            'season': self._extract_season(bet),
            'market_type': market_type,  # Now properly formatted!
            'event_string': bet.get('event', ''),
            'line_value': classification.line_value,
            'game_date': bet.get('game_date', ''),
            'direction': classification.direction,
            'stat_type': stat_type
        }

        logger.info(f"Calling R resolver for player stat: {params}")

        # Call R script
        r_result = self.r_resolver.resolve_player_stat(**params)

        if not r_result.get('success'):
            error_msg = r_result.get('error', '')
            if self._is_void_error(error_msg):
                return self._void_bet(bet)
            return r_result

        # Process result
        won, profit = self._process_player_stat_result(r_result, bet, classification)

        # Update database
        self.db.update_bet_result(bet['id'], won, profit)
        self.bankroll += profit if won else -bet['stake']

        return {
            'success': True,
            'resolved': True,
            'won': won,
            'profit': profit,
            'classification': classification.category,
            'actual_value': r_result.get('actual_value') or r_result.get('data', {}).get('actual_value')
        }

    def _resolve_scorer(self, bet: Dict, classification: MarketClassification) -> Dict:
        """Resolve scorer markets"""
        params = {
            'player_name': classification.clean_player,
            'sport': bet.get('sport', '').lower(),
            'season': self._extract_season(bet),
            'scorer_type': classification.subcategory,
            'event_string': bet.get('event', ''),
            'game_date': bet.get('game_date', '')
        }

        logger.info(f"Calling R resolver for scorer: {params}")

        # Call scorer-specific R script
        r_result = self.r_resolver.resolve_scorer(**params)

        if not r_result.get('success'):
            error_msg = r_result.get('error', '')
            if self._is_void_error(error_msg):
                return self._void_bet(bet)
            return r_result

        # Process scorer result
        won, profit = self._process_scorer_result(r_result, bet, classification)

        # Update database
        self.db.update_bet_result(bet['id'], won, profit)
        self.bankroll += profit if won else -bet['stake']

        return {
            'success': True,
            'resolved': True,
            'won': won,
            'profit': profit,
            'classification': classification.category
        }

    def _resolve_game_market(self, bet: Dict, classification: MarketClassification) -> Dict:
        """Resolve game markets (totals, spreads, moneylines)"""
        params = {
            'event_string': bet.get('event', ''),
            'sport': bet.get('sport', '').lower(),
            'season': self._extract_season(bet),
            'market_type': classification.subcategory,
            'line_value': classification.line_value,
            'direction': classification.direction,
            'team': classification.clean_team,
            'game_date': bet.get('game_date', '')
        }

        logger.info(f"Calling R resolver for game market: {params}")

        # Call game market R script
        r_result = self.r_resolver.resolve_game_market(**params)

        if not r_result.get('success'):
            error_msg = r_result.get('error', '')
            if self._is_void_error(error_msg):
                return self._void_bet(bet)
            return r_result

        # Process game result
        won, profit = self._process_game_result(r_result, bet, classification)

        # Update database
        self.db.update_bet_result(bet['id'], won, profit)
        self.bankroll += profit if won else -bet['stake']

        return {
            'success': True,
            'resolved': True,
            'won': won,
            'profit': profit,
            'classification': classification.category
        }

    def _extract_season(self, bet: Dict) -> int:
        """Extract season from bet for all sports"""
        game_date = bet.get('game_date')
        sport = bet.get('sport', '').lower()

        # If no game date, use current date logic
        if not game_date:
            current_date = datetime.now()
            current_year = current_date.year
            current_month = current_date.month

            # For sports that span calendar years
            if sport in ["nfl", "ncaaf"]:
                # NFL season runs Sep-Feb, so Jan-Feb 2026 belong to 2025 season
                if current_month <= 2:
                    return current_year - 1
                else:
                    return current_year
            elif sport in ["nba", "nhl", "ncaab", "ncaaw"]:
                # NBA/NHL run Oct-Jun, so Jan-Jun 2026 belong to 2025 season
                if current_month <= 6:
                    return current_year - 1
                else:
                    return current_year
            else:
                return current_year

        try:
            game_date_obj = datetime.strptime(game_date, "%Y-%m-%d").date()
            game_year = game_date_obj.year
            game_month = game_date_obj.month

            # NFL/NCAAF: Season runs Sep-Feb (playoffs in Jan-Feb belong to previous year)
            if sport in ["nfl", "ncaaf"]:
                if game_month >= 9:
                    return game_year
                elif game_month <= 2:
                    return game_year - 1
                else:
                    return game_year - 1

            # NBA/NHL/NCAAB/NCAAW: Season runs Oct-Jun
            elif sport in ["nba", "nhl", "ncaab", "ncaaw"]:
                if game_month >= 10:
                    return game_year
                else:
                    return game_year - 1

            # MLB/WNBA: Season runs within calendar year
            elif sport in ["mlb", "wnba"]:
                return game_year

            else:
                return game_year

        except Exception as e:
            logger.warning(f"Could not parse game date '{game_date}': {e}")
            current_year = datetime.now().year
            current_month = datetime.now().month

            # Fallback logic
            if sport in ["nfl", "ncaaf"] and current_month <= 2:
                return current_year - 1
            elif sport in ["nba", "nhl", "ncaab", "ncaaw"] and current_month <= 6:
                return current_year - 1
            return current_year

    def _process_player_stat_result(self, r_result: Dict, bet: Dict,
                                    classification: MarketClassification) -> Tuple[bool, float]:
        """Process player stat result from R"""
        stake = float(bet.get('stake', 0))
        odds = float(bet.get('odds', 0))

        # Try to get actual value from various possible locations in result
        actual_value = None

        # Check direct fields
        if 'actual_value' in r_result:
            actual_value = r_result['actual_value']
        elif 'data' in r_result and isinstance(r_result['data'], dict):
            if 'actual_value' in r_result['data']:
                actual_value = r_result['data']['actual_value']
            elif 'value' in r_result['data']:
                actual_value = r_result['data']['value']
            elif 'stats' in r_result['data'] and classification.stat_type:
                stats = r_result['data']['stats']
                if isinstance(stats, dict) and classification.stat_type in stats:
                    actual_value = stats[classification.stat_type]

        if actual_value is None:
            logger.warning(f"Could not extract actual value from R result")
            actual_value = 0

        line_value = classification.line_value
        direction = classification.direction

        # Determine if bet won
        if direction == 'over':
            if line_value is not None:
                won = actual_value > line_value
            else:
                won = actual_value > 0
        elif direction == 'under':
            if line_value is not None:
                won = actual_value < line_value
            else:
                won = actual_value < 0
        else:
            # Boolean market (like double double)
            if line_value is not None:
                won = actual_value > line_value
            else:
                won = actual_value > 0.5

        profit = stake * (odds - 1) if won else -stake
        logger.info(f"Player stat result: actual={actual_value}, line={line_value}, direction={direction}, won={won}")
        return won, profit

    def _process_scorer_result(self, r_result: Dict, bet: Dict,
                               classification: MarketClassification) -> Tuple[bool, float]:
        """Process scorer result from R"""
        stake = float(bet.get('stake', 0))
        odds = float(bet.get('odds', 0))

        # Check if player was the scorer
        player_scored = False

        if 'player_scored' in r_result:
            player_scored = r_result['player_scored']
        elif 'data' in r_result and isinstance(r_result['data'], dict):
            if 'player_scored' in r_result['data']:
                player_scored = r_result['data']['player_scored']
            elif 'won' in r_result['data']:
                player_scored = r_result['data']['won']

        won = player_scored
        profit = stake * (odds - 1) if won else -stake
        logger.info(f"Scorer result: player_scored={player_scored}, won={won}")
        return won, profit

    def _process_game_result(self, r_result: Dict, bet: Dict,
                             classification: MarketClassification) -> Tuple[bool, float]:
        """Process game market result from R"""
        stake = float(bet.get('stake', 0))
        odds = float(bet.get('odds', 0))

        data = r_result.get('data', {})

        # Try to get win status from various possible fields
        won = False

        if 'won' in r_result:
            won = r_result['won']
        elif 'bet_won' in r_result:
            won = r_result['bet_won']
        elif 'data' in r_result and isinstance(r_result['data'], dict):
            if 'won' in r_result['data']:
                won = r_result['data']['won']
            elif 'bet_won' in r_result['data']:
                won = r_result['data']['bet_won']
            elif 'winner' in r_result['data']:
                # Check if our team won
                winner = r_result['data']['winner']
                if winner == 'home' and classification.clean_team == data.get('home_team'):
                    won = True
                elif winner == 'away' and classification.clean_team == data.get('away_team'):
                    won = True

        profit = stake * (odds - 1) if won else -stake
        logger.info(f"Game result: won={won}")
        return won, profit

    def automated_scanning(self, max_scans: int = 2):
        """Enhanced automated scanning with intelligent betting"""
        logger.info(f"Starting enhanced automated scanning ({max_scans} scans)")

        for scan_num in range(1, max_scans + 1):
            logger.info(f"\nScan #{scan_num}/{max_scans} | Bankroll: €{self.bankroll:.2f}")

            # Scan for opportunities
            opportunities = self.scanner.scrape_crazyninja_odds()

            # Get current pending bets
            pending_bets = self.db.get_pending_bets()

            # Process opportunities with intelligent filtering
            filtered_opps = self.arbitrage_system.process_opportunities(
                opportunities, self.bankroll, pending_bets
            )

            # Place bets intelligently
            bets_placed = 0
            for opp in filtered_opps[:self.config.betting.max_bets_per_scan]:
                if self.place_intelligent_bet(opp):
                    bets_placed += 1

            # Try to resolve pending bets
            if pending_bets:
                logger.info(f"Checking for completed games...")
                for bet in pending_bets:
                    result = self.resolve_bet_intelligently(bet['id'])
                    if result.get('resolved'):
                        logger.info(f"  Resolved bet {bet['id']}: {'WIN' if result['won'] else 'LOSS'}")

            # Update bankroll from database
            stats = self.db.get_performance_stats()
            self.bankroll = self.config.bankroll.initial_bankroll + stats.get('total_profit', 0)

            logger.info(f"Status: {len(self.db.get_pending_bets())} pending bets")

        logger.info("Automated scanning completed")

    def run(self):
        """Main menu - slimmed down version"""
        while True:
            pending_count = len(self.db.get_pending_bets())

            print("\n" + "=" * 60)
            print(f"ENHANCED PAPER TRADING SYSTEM")
            print("=" * 60)
            print(f"Current Bankroll: €{self.bankroll:.2f}")
            print(f"Pending Bets: {pending_count}")
            print(f"Market Classifier: ACTIVE")
            print()
            print("1. Intelligent Automated Scanning")
            print("2. Single Manual Scan & Bet")
            print("3. View Performance Dashboard")
            print("4. View Recent Bets")
            print("5. Resolve Pending Bets")
            print("6. Test Market Classifier")
            print("7. Debug Tools")
            print("8. Fix Missing Bet Categories")
            print("0. Exit")
            print()

            choice = input("Select option: ").strip()

            if choice == '1':
                self.automated_scanning(max_scans=2)
            elif choice == '2':
                self._single_manual_scan()
            elif choice == '3':
                self._view_performance()
            elif choice == '4':
                self._view_recent_bets()
            elif choice == '5':
                self._resolve_pending()
            elif choice == '6':
                self._test_classifier()
            elif choice == '7':
                self._debug_menu()
            elif choice == "8":
                print("\n=== FIXING MISSING BET CATEGORIES ===")
                updated = self.db.update_existing_bet_categories()
                print(f"✓ Updated {updated} bets with missing categories")
            elif choice == '0':
                print("Exiting...")
                break
            else:
                print("Invalid option")

    def _single_manual_scan(self):
        """Single manual scan"""
        print("\nManual scan...")
        opportunities = self.scanner.scrape_crazyninja_odds()

        for opp in opportunities[:self.config.betting.max_bets_per_scan]:
            # Check for college games
            event = opp.get('event', '')
            sport = opp.get('sport', 'unknown')

            if self._is_college_game(event, sport):
                print(f"\n{opp['player']} - {opp['market']}")
                print(f"Skipping college game: {event}")
                continue

            # Classify and show info
            classification = self.market_classifier.classify(opp['market'], opp['player'])
            print(f"\n{opp['player']} - {opp['market']}")
            print(f"  Classified as: {classification.category}.{classification.subcategory}")
            print(f"  Sport: {sport}")
            print(f"  Stat Type: {classification.stat_type}")
            print(f"  Direction: {classification.direction}, Line: {classification.line_value}")

            # Ask user if they want to place bet
            place = input("  Place bet? (y/N): ").strip().lower()
            if place == 'y':
                self.place_intelligent_bet(opp)

    def _view_performance(self):
        """View performance dashboard"""
        stats = self.db.get_performance_stats()

        print(f"\nPERFORMANCE DASHBOARD")
        print("=" * 40)
        print(f"Bankroll: €{self.bankroll:.2f}")
        print(f"Total Profit/Loss: €{stats['total_profit']:.2f}")
        print(f"Total Bets: {stats['total_bets']}")
        print(f"Won: {stats['won_bets']} | Lost: {stats['lost_bets']}")
        print(f"Pending: {stats['pending_bets']}")
        print(f"Win Rate: {stats['win_rate']:.1f}%")

    def _view_recent_bets(self):
        """View recent bets"""
        recent_bets = self.db.get_bet_history(limit=10)

        print(f"\nRECENT BETS")
        print("=" * 70)
        for bet in recent_bets:
            status = bet.get('status', 'pending').upper()
            player = bet.get('player', 'Unknown')
            market = bet.get('market', 'Unknown')
            category = bet.get('market_category', 'N/A')
            sport = bet.get('sport', 'Unknown')
            stat_type = bet.get('market_stat_type', 'N/A')

            print(f"{status} - {player}")
            print(f"  Market: {market} | Sport: {sport} | Category: {category} | Stat: {stat_type}")
            if bet['status'] != 'pending':
                print(f"  Profit: €{bet['profit']:.2f}")
            print()

    def _resolve_pending(self):
        """Resolve pending bets"""
        pending_bets = self.db.get_pending_bets()

        if not pending_bets:
            print("No pending bets")
            return

        print(f"\nResolving {len(pending_bets)} pending bets...")

        resolved_count = 0
        future_count = 0
        college_count = 0
        void_count = 0  # Added void count
        error_count = 0

        for bet in pending_bets:
            player = bet.get('player', 'Unknown')
            market = bet.get('market', 'Unknown')
            game_date = bet.get('game_date', 'Unknown')
            sport = bet.get('sport', 'Unknown')
            stat_type = bet.get('market_stat_type', 'N/A')

            print(f"\n  Bet: {player} - {market}")
            print(f"    Game: {game_date} | Sport: {sport} | Stat: {stat_type}")

            result = self.resolve_bet_intelligently(bet['id'])

            if not result.get('success'):
                error_msg = result.get('error', 'Unknown error')
                print(f"    ✗ Error: {error_msg}")
                error_count += 1
            elif result.get('void'):
                void_reason = result.get('void_reason', 'Unknown')
                stake_returned = result.get('stake_returned', 0)
                print(f"    ⚠️ VOID: {void_reason} - Stake returned: €{stake_returned:.2f}")
                void_count += 1
            elif result.get('resolved'):
                status = "WON" if result.get('won') else "LOST"
                profit = result.get('profit', 0)
                actual = result.get('actual_value', 'N/A')
                print(f"    ✓ {status}: €{profit:.2f} (Actual: {actual})")
                resolved_count += 1
            elif result.get('skip_reason') == 'future_game':
                message = result.get('message', 'Game in future')
                print(f"    ⏸ Skipped (future): {message}")
                future_count += 1
            elif result.get('skip_reason') == 'college_game':
                message = result.get('message', 'College game')
                print(f"    🎓 Skipped (college): {message}")
                college_count += 1
            else:
                # Other reasons (like no data yet)
                message = result.get('message', 'Not yet resolvable')
                print(f"    ○ Not resolved: {message}")
                error_count += 1

        print(f"\nSummary:")
        print(f"  Resolved: {resolved_count}")
        print(f"  Voided: {void_count}")
        print(f"  Future games (skipped): {future_count}")
        print(f"  College games (skipped): {college_count}")
        print(f"  Errors/unresolved: {error_count}")

        if resolved_count == 0 and (future_count > 0 or college_count > 0 or void_count > 0):
            print("\nNote: Most bets are for future, college, or voided games. They will not be resolved.")

    def _test_classifier(self):
        """Test the market classifier"""
        print("\nMARKET CLASSIFIER TEST")
        print("=" * 40)

        test_cases = [
            # NFL
            ("Jeremy Ruckert Last Touchdown Scorer", "Player Last Touchdown Scorer"),
            ("Patrick Mahomes Over 275.5 Passing Yards", "Player Passing Yards Over"),

            # NBA
            ("LeBron James Over 25.5 Points", "Player Points Over"),

            # MLB
            ("Aaron Judge Over 1.5 Hits", "Player Hits Over"),
            ("Shohei Ohtani Over 0.5 Home Runs", "Player Home Runs Over"),

            # WNBA
            ("A'ja Wilson Over 20.5 Points", "Player Points Over"),

            # NCAAF
            ("Caleb Williams Over 275.5 Passing Yards", "Player Passing Yards Over"),

            # NCAAB
            ("Zach Edey Over 22.5 Points", "Player Points Over"),
            ("Zach Edey Double Double", "Player Double Double"),

            # Game markets
            ("Chiefs Moneyline", "Moneyline"),
            ("Dolphins @ Jets Over 45.5", "Total Points Over"),
        ]

        for player, market in test_cases:
            classification = self.market_classifier.classify(market, player)
            print(f"\nTest: {player} - {market}")
            print(f"  Category: {classification.category}")
            print(f"  Subcategory: {classification.subcategory}")
            print(f"  Direction: {classification.direction}")
            print(f"  Line: {classification.line_value}")
            print(f"  Stat Type: {classification.stat_type}")
            print(f"  Resolution Type: {self.market_classifier.get_resolution_type(classification)}")

    def _debug_menu(self):
        """Debug tools menu"""
        while True:
            print("\nDEBUG TOOLS")
            print("=" * 30)
            print("1. Test R Resolver")
            print("2. Check R Packages")
            print("3. Test Exact Pending Bet")
            print("4. Back to Main Menu")

            choice = input("\nSelect option: ").strip()

            if choice == '1':
                self._test_r_resolver()
            elif choice == '2':
                self._check_r_packages()
            elif choice == '3':
                self._test_exact_pending()
            elif choice == '4':
                break
            else:
                print("Invalid option")

    def _test_r_resolver(self):
        """Test R resolver directly"""
        print("\nTesting R resolver...")

        bet_id = input("Enter bet ID to test (or leave empty for sample): ").strip()

        if bet_id:
            result = self.resolve_bet_intelligently(bet_id)
            print(f"Result: {json.dumps(result, indent=2, default=str)}")
        else:
            # Test with a sample MLB bet
            print("\nTesting sample MLB bet...")
            sample_result = self.r_resolver.resolve_player_stat(
                player_name="Aaron Judge",
                sport="mlb",
                season=2025,
                market_type="player home runs over",
                event_string="New York Yankees @ Boston Red Sox",
                line_value=0.5,
                game_date="2025-10-01",
                direction="over",
                stat_type="home_runs"
            )
            print(f"Sample result: {json.dumps(sample_result, indent=2, default=str)}")

    def _check_r_packages(self):
        """Check R packages"""
        print("\nChecking R packages...")

        # This would call R to check if required packages are installed
        # For now, just print a message
        print("R packages required: nflreadr, hoopR, jsonlite, stringr, dplyr, lubridate, httr")
        print("Your R script should handle package installation automatically.")

    def _test_exact_pending(self):
        """Test exact pending bet"""
        print("\nTesting exact pending bet...")

        bet_id = input("Enter bet ID: ").strip()
        if not bet_id:
            print("No bet ID provided")
            return

        # Get bet details
        bet = self.db.get_bet(bet_id)
        if not bet:
            print(f"Bet {bet_id} not found")
            return

        print(f"\nBet details:")
        print(f"  Player: {bet.get('player')}")
        print(f"  Market: {bet.get('market')}")
        print(f"  Sport: {bet.get('sport')}")
        print(f"  Game Date: {bet.get('game_date')}")

        # Test resolution
        print("\nAttempting resolution...")
        result = self.resolve_bet_intelligently(bet_id)
        print(f"\nResult: {json.dumps(result, indent=2, default=str)}")