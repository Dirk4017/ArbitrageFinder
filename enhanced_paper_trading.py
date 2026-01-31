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
from typing import Dict, List, Optional, Tuple, Any  # <-- ADD THIS!

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
            from datetime import datetime

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

        # College football indicators
        college_keywords = [
            'oregon', 'texas tech', 'ncaa', 'college',
            'clemson', 'alabama', 'georgia', 'michigan', 'ohio state',
            'usc', 'stanford', 'florida state', 'lsu', 'oklahoma',
            'notre dame', 'pen state', 'tennessee', 'utah',
            'wisconsin', 'iowa', 'michigan state', 'penn state',
            'texas a&m', 'auburn', 'florida', 'miami',
            # Add more as needed
        ]

        # Check if this looks like college football
        if sport_lower in ['football', 'nfl', 'cfb']:
            for keyword in college_keywords:
                if keyword in event_lower:
                    return True

        # Check for NCAA/FBS/FCS indicators
        if any(college_marker in event_lower for college_marker in
               ['ncaa', 'fbs', 'fcs', 'college', 'cfb', 'pac-12', 'big ten', 'sec', 'acc', 'big 12']):
            return True

        return False

    def place_intelligent_bet(self, opportunity: Dict) -> bool:
        """Place a bet with intelligent market classification"""
        try:
            # Check for college games
            event = opportunity.get('event', '')
            sport = opportunity.get('sport', 'unknown')

            if self._is_college_game(event, sport):
                logger.warning(f"Skipping college game: {event}")
                return False

            # 1. Classify the market FIRST
            classification = self.market_classifier.classify(
                market=opportunity['market'],
                player_or_team=opportunity['player']
            )

            logger.info(f"Market classified as: {classification.category}")

            # 2. Get Kelly stake based on classification
            pending_bets = self.db.get_pending_bets()
            decimal_odds = self.arbitrage_system.parse_odds(opportunity.get('odds', '1.0'))

            if decimal_odds is None:
                logger.warning(f"Could not parse odds: {opportunity.get('odds')}")
                return False

            stake = self.arbitrage_system.calculate_stake(
                ev=opportunity['ev'],
                bankroll=self.bankroll,
                pending_bets=pending_bets,
                odds=decimal_odds,
                market_classification=classification  # Pass classification for better risk assessment
            )

            if stake <= 0:
                logger.info(f"Stake too low: {stake}")
                return False

            # 3. Prepare bet data with classification info
            bet_data = {
                'event': opportunity['event'],
                'sport': opportunity.get('sport', 'unknown'),
                'market': opportunity['market'],
                'player': opportunity['player'],
                'odds': decimal_odds,
                'stake': stake,
                'potential_win': stake * decimal_odds,
                'ev': opportunity['ev'],
                'sportsbook': opportunity.get('sportsbook', 'Unknown'),
                'game_date': opportunity.get('game_date'),
                'market_category': classification.category,  # Store for resolution
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

            logger.info(f"Bet placed: {opportunity['player']} - {opportunity['market']}")
            logger.info(f"  Stake: €{stake:.2f}, Classification: {classification.category}")

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

            # Route to appropriate resolver
            resolution_type = self.market_classifier.get_resolution_type(classification)

            if resolution_type == 'player_stat':
                return self._resolve_player_stat(bet, classification)
            elif resolution_type == 'scorer':
                return self._resolve_scorer(bet, classification)
            elif resolution_type == 'game':
                return self._resolve_game_market(bet, classification)
            else:
                # Fallback to idempotent resolver
                return self.idempotent_resolver.resolve_bet_safe(bet_id)

        except Exception as e:
            logger.error(f"Intelligent resolution failed: {e}")
            return {'success': False, 'error': str(e), 'resolved': False}

    def _resolve_player_stat(self, bet: Dict, classification: MarketClassification) -> Dict:
        """Resolve player stat markets"""
        # Prepare parameters for R
        params = {
            'player_name': classification.clean_player,
            'sport': bet.get('sport', '').lower(),
            'season': self._extract_season(bet),
            'market_type': classification.subcategory,
            'event_string': bet.get('event', ''),
            'line_value': classification.line_value,
            'game_date': bet.get('game_date', ''),
            'direction': classification.direction,
            'stat_type': classification.stat_type
        }

        # Call R script
        r_result = self.r_resolver.resolve_player_stat(**params)

        if not r_result.get('success'):
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
            'classification': classification.category
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

        # Call scorer-specific R script
        r_result = self.r_resolver.resolve_scorer(**params)

        if not r_result.get('success'):
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

        # Call game market R script
        r_result = self.r_resolver.resolve_game_market(**params)

        if not r_result.get('success'):
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
        """Extract season from bet"""
        # Use your existing logic from RStatsResolver._extract_season_hint
        game_date = bet.get('game_date')
        sport = bet.get('sport', '').lower()

        if game_date:
            try:
                from datetime import datetime
                game_date_obj = datetime.strptime(game_date, "%Y-%m-%d").date()

                if sport == "nfl":
                    # NFL season: Games in Jan-Feb belong to previous year
                    if game_date_obj.month <= 2:
                        return game_date_obj.year - 1
                    else:
                        return game_date_obj.year
                elif sport == "nba":
                    # NBA season: Games Oct-June, spanning years
                    if game_date_obj.month >= 10:
                        return game_date_obj.year
                    else:
                        return game_date_obj.year - 1
            except:
                pass

        # Fallback to current season
        current_year = datetime.now().year
        if sport == "nfl" and datetime.now().month <= 2:
            return current_year - 1
        return current_year


    def _process_player_stat_result(self, r_result: Dict, bet: Dict,
                                    classification: MarketClassification) -> Tuple[bool, float]:
        """Process player stat result from R"""
        # Your existing logic from _resolve_comprehensive_market
        stake = float(bet.get('stake', 0))
        odds = float(bet.get('odds', 0))

        actual_value = r_result.get('data', {}).get('actual_value', 0)
        line_value = classification.line_value
        direction = classification.direction

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
            # Boolean market
            won = actual_value > (line_value or 0.5)

        profit = stake * (odds - 1) if won else -stake
        return won, profit

    def _process_scorer_result(self, r_result: Dict, bet: Dict,
                               classification: MarketClassification) -> Tuple[bool, float]:
        """Process scorer result from R"""
        stake = float(bet.get('stake', 0))
        odds = float(bet.get('odds', 0))

        # Check if player was the scorer
        player_scored = r_result.get('data', {}).get('player_scored', False)
        won = player_scored

        profit = stake * (odds - 1) if won else -stake
        return won, profit

    def _process_game_result(self, r_result: Dict, bet: Dict,
                             classification: MarketClassification) -> Tuple[bool, float]:
        """Process game market result from R"""
        stake = float(bet.get('stake', 0))
        odds = float(bet.get('odds', 0))

        # Your existing logic from _resolve_game_market
        data = r_result.get('data', {})

        if classification.subcategory == 'moneyline':
            team_won = data.get('team_won', False)
            won = team_won
        elif classification.subcategory == 'total':
            total = data.get('total_points', 0)
            line_value = classification.line_value
            direction = classification.direction

            # FIXED: Check line_value before comparison
            if line_value is not None:
                if direction == 'over':
                    won = total > line_value
                elif direction == 'under':
                    won = total < line_value
                else:
                    won = False
            else:
                won = False  # Can't resolve without line value
        elif classification.subcategory == 'spread':
            spread_result = data.get('spread_result', False)
            won = spread_result
        else:
            won = False

        profit = stake * (odds - 1) if won else -stake
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
            # In the menu options, add something like:
            elif choice == "8":  # Or whatever number is available
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
                print(f"  ⚠️ Skipping college game: {event}")
                continue

            # Classify and show info
            classification = self.market_classifier.classify(opp['market'], opp['player'])
            print(f"\n{opp['player']} - {opp['market']}")
            print(f"  Classified as: {classification.category}.{classification.subcategory}")

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

            print(f"{status} - {player}")
            print(f"  Market: {market} | Sport: {sport} | Category: {category}")
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
        error_count = 0

        for bet in pending_bets:
            player = bet.get('player', 'Unknown')
            market = bet.get('market', 'Unknown')
            game_date = bet.get('game_date', 'Unknown')
            sport = bet.get('sport', 'Unknown')

            print(f"\n  Bet: {player} - {market}")
            print(f"    Game: {game_date} | Sport: {sport}")

            result = self.resolve_bet_intelligently(bet['id'])

            if not result.get('success'):
                error_msg = result.get('error', 'Unknown error')
                print(f"    ✗ Error: {error_msg}")
                error_count += 1
            elif result.get('resolved'):
                status = "WON" if result.get('won') else "LOST"
                profit = result.get('profit', 0)
                print(f"    ✓ {status}: €{profit:.2f}")
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
        print(f"  Future games (skipped): {future_count}")
        print(f"  College games (skipped): {college_count}")
        print(f"  Errors/unresolved: {error_count}")

        if resolved_count == 0 and (future_count > 0 or college_count > 0):
            print("\nNote: Most bets are for future or college games. They will not be resolved.")

    def _test_classifier(self):
        """Test the market classifier"""
        print("\nMARKET CLASSIFIER TEST")
        print("=" * 40)

        test_cases = [
            ("Jeremy Ruckert Last Touchdown Scorer", "Player Last Touchdown Scorer"),
            ("Patrick Mahomes Over 275.5 Passing Yards", "Player Passing Yards Over"),
            ("LeBron James Over 25.5 Points", "Player Points Over"),
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
        # Use your existing debug_test_r_script method
        print("\nTesting R resolver...")
        # You can adapt your existing debug methods here

    def _check_r_packages(self):
        """Check R packages"""
        # Use your existing check_r_packages method
        print("\nChecking R packages...")

    def _test_exact_pending(self):
        """Test exact pending bet"""
        # Use your existing test_exact_pending_bet method
        print("\nTesting exact pending bet...")