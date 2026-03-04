"""
Idempotent Resolution Manager - Production-grade state machine for bet resolution
"""
import time
import logging
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class IdempotentResolutionManager:
    """
    Production-grade state machine for bet resolution
    Guarantees: no double resolutions, safe retries, audit trail
    """

    def __init__(self, db, r_resolver):
        self.db = db
        self.resolver = r_resolver

        # State definitions
        self.STATES = {
            'PENDING': 'pending',
            'RESOLVING': 'resolving',
            'COMPLETE': 'complete',
            'FAILED_PERMANENT': 'failed_permanent',
            'RETRY_PENDING': 'retry_pending',
            'VOID': 'void'  # Added VOID state
        }

        # Audit trail
        self.audit_log = []

    def _format_market_type_for_r(self, market_type: str, sport: str, stat_type: str = None) -> str:
        """Format market type to what R script expects"""
        if not market_type:
            return market_type

        market_lower = market_type.lower()
        sport_lower = sport.lower() if sport else ''

        # NBA formats
        if sport_lower in ['nba', 'basketball', 'wnba', 'ncaab', 'ncaaw']:
            if stat_type == 'points' or 'points' in market_lower:
                return "Player Points"
            elif stat_type == 'rebounds' or 'rebounds' in market_lower:
                return "Player Rebounds"
            elif stat_type == 'assists' or 'assists' in market_lower:
                return "Player Assists"
            elif stat_type == 'three_pointers_made' or any(x in market_lower for x in ['threes', '3pt', 'three point']):
                return "threes"
            elif 'points + assists' in market_lower or 'points+assists' in market_lower:
                return "Points + Assists"
            elif 'points + rebounds' in market_lower or 'points+rebounds' in market_lower:
                return "Points + Rebounds"
            elif 'rebounds + assists' in market_lower or 'rebounds+assists' in market_lower:
                return "Rebounds + Assists"
            elif any(x in market_lower for x in ['pra', 'points+rebounds+assists', 'points + rebounds + assists']):
                return "Points + Rebounds + Assists"

        # NFL formats
        elif sport_lower in ['nfl', 'football']:
            if stat_type == 'passing_yards' or 'passing yards' in market_lower:
                return "Player Passing Yards"
            elif stat_type == 'receiving_yards' or 'receiving yards' in market_lower:
                return "Player Receiving Yards"
            elif stat_type == 'rushing_yards' or 'rushing yards' in market_lower:
                return "Player Rushing Yards"
            elif stat_type == 'receptions' or 'receptions' in market_lower:
                return "Player Receptions"
            elif stat_type == 'touchdowns' or 'touchdown' in market_lower:
                return "Player Touchdowns"

        # MLB formats
        elif sport_lower in ['mlb', 'baseball']:
            if stat_type == 'hits' or 'hits' in market_lower:
                return "Player Hits"
            elif stat_type == 'home_runs' or 'home runs' in market_lower:
                return "Player Home Runs"
            elif stat_type == 'rbi' or 'rbi' in market_lower:
                return "Player RBI"

        # NHL formats
        elif sport_lower in ['nhl', 'hockey']:
            if stat_type == 'goals' or 'goals' in market_lower:
                return "Player Goals"
            elif stat_type == 'assists' or 'assists' in market_lower:
                return "Player Assists"
            elif stat_type == 'shots' or any(x in market_lower for x in ['shots', 'sog']):
                return "Player Shots On Goal"
            elif stat_type == 'saves' or 'saves' in market_lower:
                return "Player Saves"

        # If no formatting applied, return original
        return market_type

    def _clean_player_name(self, player_name: str) -> str:
        """Clean player name by removing Over/Under and line values"""
        if not player_name:
            return ""

        # Remove "Over X.X", "Under X.X", "O X.X", "U X.X"
        cleaned = re.sub(r'\s+(Over|Under|O|U)\s+\d+\.?\d*', '', player_name, flags=re.IGNORECASE)
        # Remove standalone numbers
        cleaned = re.sub(r'\s+\d+\.?\d*', '', cleaned)
        # Remove extra spaces
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()

        return cleaned

    def _determine_win(self, result: Dict, bet: Dict, line_value: Optional[float],
                       direction: Optional[str]) -> Tuple[Optional[bool], Optional[str]]:
        """
        Determine if bet won based on result data
        Returns: (won, void_reason) - if void_reason is not None, bet should be voided
        """
        # DEBUG: Log the full result to see what we're getting
        logger.info(f"========== DEBUG _determine_win for bet {bet.get('id')} ==========")
        logger.info(f"DEBUG result keys: {list(result.keys())}")
        logger.info(f"DEBUG result.get('success'): {result.get('success')}")
        logger.info(f"DEBUG result.get('resolved'): {result.get('resolved')}")
        logger.info(f"DEBUG result.get('error'): {result.get('error')}")

        if 'data' in result:
            logger.info(
                f"DEBUG data keys: {list(result['data'].keys()) if isinstance(result['data'], dict) else 'not a dict'}")

        # Check for void conditions first
        if not result.get('success'):
            error = result.get('error', '')
            logger.info(f"DEBUG checking error string: '{error}'")
            logger.info(f"DEBUG error.lower(): '{error.lower()}'")

            # Check each condition separately
            no_player_stats = 'no player stats' in error.lower()
            player_not_found = 'player not found' in error.lower()
            no_stats = 'no stats' in error.lower()

            logger.info(f"DEBUG 'no player stats' in error: {no_player_stats}")
            logger.info(f"DEBUG 'player not found' in error: {player_not_found}")
            logger.info(f"DEBUG 'no stats' in error: {no_stats}")

            if error and (no_player_stats or player_not_found or no_stats):
                logger.info(f"✅ DEBUG void condition MATCHED! Returning None, 'player_did_not_play'")
                return None, 'player_did_not_play'
            else:
                logger.info(f"❌ DEBUG void condition NOT matched")

        # Check result for void indicators even if success=True
        if result.get('success'):
            error = result.get('error', '')
            if error and ('no player stats' in error.lower() or
                          'player not found' in error.lower()):
                logger.info(f"✅ DEBUG void condition matched in success case")
                return None, 'player_did_not_play'

            # Check data field for void indicators
            data = result.get('data', {})
            if isinstance(data, dict):
                if data.get('found') is False:
                    logger.info(f"✅ DEBUG void condition matched: found=False")
                    return None, 'player_not_found'

                # Check if stats exist but are all zeros (player played 0 minutes)
                if 'stats' in data:
                    stats = data['stats']
                    # If minutes is 0 or None, player didn't play
                    if 'minutes' in stats and (stats['minutes'] == 0 or stats['minutes'] is None):
                        logger.info(f"✅ DEBUG void condition matched: minutes=0")
                        return None, 'player_did_not_play'

        # Also check for void in the raw result even if success is weird
        if 'error' in result and result['error']:
            error = result['error']
            if ('no player stats' in error.lower() or
                    'player not found' in error.lower()):
                logger.info(f"✅ DEBUG void condition matched from raw error field")
                return None, 'player_did_not_play'

        # Get line value from multiple sources
        line = line_value or bet.get('line_value') or bet.get('market_line_value')
        logger.info(f"DEBUG line value: {line}")

        # Get direction from multiple sources
        bet_direction = direction or bet.get('direction') or bet.get('market_direction', 'over')
        logger.info(f"DEBUG direction: {bet_direction}")

        # Case 1: Check if result has bet_won field
        if 'bet_won' in result:
            logger.info(f"DEBUG found bet_won field: {result['bet_won']}")
            return result['bet_won'], None

        # Try to extract actual value from result
        actual = None
        logger.info(f"DEBUG attempting to extract actual value...")

        # Check direct actual_value field
        if 'actual_value' in result:
            actual = result['actual_value']
            logger.info(f"DEBUG found actual_value in result: {actual}")

        # Check in data field
        elif 'data' in result and isinstance(result['data'], dict):
            data = result['data']
            logger.info(f"DEBUG checking data dict with keys: {list(data.keys())}")

            if 'actual_value' in data:
                actual = data['actual_value']
                logger.info(f"DEBUG found actual_value in data: {actual}")
            elif 'stats' in data:
                stats = data['stats']
                logger.info(
                    f"DEBUG checking stats dict with keys: {list(stats.keys()) if isinstance(stats, dict) else 'not a dict'}")

                # Try to find the relevant stat based on market type
                if bet.get('market_stat_type') and isinstance(stats, dict) and bet['market_stat_type'] in stats:
                    actual = stats[bet['market_stat_type']]
                    logger.info(f"DEBUG found stat by market_stat_type '{bet['market_stat_type']}': {actual}")
                else:
                    # Try common stat types
                    for stat in ['points', 'rebounds', 'assists', 'steals', 'blocks',
                                 'passing_yards', 'receiving_yards', 'rushing_yards',
                                 'receptions', 'touchdowns', 'total_tds', 'hits',
                                 'home_runs', 'rbi', 'goals', 'shots']:
                        if isinstance(stats, dict) and stat in stats:
                            actual = stats[stat]
                            logger.info(f"DEBUG found stat '{stat}': {actual}")
                            break

        # If we have both actual and line, determine win/loss
        if actual is not None and line is not None:
            try:
                line_val = float(line)
                logger.info(f"DEBUG comparing actual={actual} vs line={line_val}")
                if bet_direction.lower() == 'over':
                    result_bool = actual > line_val
                    logger.info(f"DEBUG over result: {result_bool}")
                    return result_bool, None
                elif bet_direction.lower() == 'under':
                    result_bool = actual < line_val
                    logger.info(f"DEBUG under result: {result_bool}")
                    return result_bool, None
            except (ValueError, TypeError) as e:
                logger.info(f"DEBUG error converting to float: {e}")

        # Case 2: Check for actual_value and line_value in result (original logic)
        if 'actual_value' in result and line is not None:
            actual = result['actual_value']
            line_val = float(line)
            logger.info(f"DEBUG (case2) actual={actual}, line={line_val}")

            if bet_direction.lower() == 'over':
                return actual > line_val, None
            elif bet_direction.lower() == 'under':
                return actual < line_val, None

        # Case 3: Check data field (original logic)
        data = result.get('data', {})
        if isinstance(data, dict):
            # Check for actual_value in data
            if 'actual_value' in data and line is not None:
                actual = data['actual_value']
                line_val = float(line)
                logger.info(f"DEBUG (case3) actual from data: {actual}")

                if bet_direction.lower() == 'over':
                    return actual > line_val, None
                elif bet_direction.lower() == 'under':
                    return actual < line_val, None

            # Check for stats in data (original logic)
            if 'stats' in data and line is not None:
                stats = data['stats']
                stat_types = ['receptions', 'receiving_yards', 'rushing_yards', 'passing_yards',
                              'points', 'assists', 'rebounds', 'touchdowns', 'total_tds',
                              'hits', 'home_runs', 'rbi', 'stolen_bases', 'goals', 'shots']

                for stat in stat_types:
                    if isinstance(stats, dict) and stat in stats:
                        actual = stats[stat]
                        if actual is not None:
                            line_val = float(line)
                            logger.info(f"DEBUG (case3) found stat '{stat}': {actual}")
                            if bet_direction.lower() == 'over':
                                return actual > line_val, None
                            elif bet_direction.lower() == 'under':
                                return actual < line_val, None

            # Check for boolean fields
            if 'won' in data:
                logger.info(f"DEBUG found 'won' in data: {data['won']}")
                return data['won'], None
            if 'player_scored' in data:
                logger.info(f"DEBUG found 'player_scored' in data: {data['player_scored']}")
                return data['player_scored'], None
            if 'achieved' in data:
                logger.info(f"DEBUG found 'achieved' in data: {data['achieved']}")
                return data['achieved'], None

        # Case 4: Check if we have line in bet and can calculate from result
        if line is not None:
            line_val = float(line)
            logger.info(f"DEBUG (case4) searching for numeric value in result")

            # Look for any numeric value in result that might be the actual
            for key in ['actual_value', 'value', 'score', 'total', 'points', 'yards', 'receptions', 'hits', 'runs',
                        'rbi', 'goals', 'assists', 'rebounds', 'shots']:
                if key in result:
                    actual = result[key]
                    if isinstance(actual, (int, float)):
                        logger.info(f"DEBUG (case4) found key '{key}': {actual}")
                        if bet_direction.lower() == 'over':
                            return actual > line_val, None
                        elif bet_direction.lower() == 'under':
                            return actual < line_val, None

        # Log warning if we can't determine
        logger.warning(f"Could not determine win/loss for bet {bet.get('id')}")
        logger.warning(f"Line value: {line}, Direction: {bet_direction}")
        logger.warning(f"Result keys: {list(result.keys())}")
        if 'data' in result and isinstance(result['data'], dict):
            logger.warning(f"Data keys: {list(result['data'].keys())}")

        # Default to loss (safer for bankroll)
        logger.info(f"DEBUG defaulting to LOSS")
        return False, None

    def resolve_bet_safe(self, bet_id: str, force: bool = False) -> Dict:
        """
        Idempotent resolution with full state tracking
        """
        resolution_id = f"res_{bet_id}_{int(time.time())}"

        logger.info(f"Starting resolution for bet {bet_id}")

        try:
            # 1. Get bet and current state
            bet = self.db.get_bet(bet_id)
            if not bet:
                return self._create_result('failed_permanent',
                                           error=f"Bet {bet_id} not found",
                                           resolution_id=resolution_id)

            # ========== DEBUGGING: Check what sport is in the database ==========
            sport_from_db = bet.get('sport', '').lower()
            logger.info(f">>> DATABASE SPORT VALUE: '{sport_from_db}' for bet {bet_id}")
            logger.info(
                f">>> Bet details - event: {bet.get('event')}, player: {bet.get('player')}, market: {bet.get('market')}")
            logger.info(f">>> Market category: {bet.get('market_category')}, game_date: {bet.get('game_date')}")
            # ====================================================================

            current_state = bet.get('resolution_state', self.STATES['PENDING'])

            # 2. Check state transitions
            if current_state == self.STATES['COMPLETE']:
                # Already resolved - return existing result
                return self._create_result(
                    'complete',
                    result=bet.get('result'),
                    profit=bet.get('profit'),
                    won=bet.get('result') == 'won',
                    resolved=True,
                    resolution_id=resolution_id
                )

            elif current_state == self.STATES['FAILED_PERMANENT']:
                # Permanent failure - no retry
                return self._create_result(
                    'failed_permanent',
                    error=bet.get('resolution_error', 'Previous permanent failure'),
                    resolved=False,
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
                        return self._create_result(
                            'retry_pending',
                            error=f"Retry cooldown active: {hours_since:.1f}h since last attempt",
                            next_retry=next_retry.isoformat(),
                            resolved=False,
                            resolution_id=resolution_id
                        )

            # 3. Attempt resolution with the R resolver
            self._update_bet_state(bet_id, self.STATES['RESOLVING'],
                                   f"Starting resolution {resolution_id}")

            try:
                # Get bet details
                sport = bet.get('sport', '').lower()

                # ========== DEBUGGING: Log the sport being used ==========
                logger.info(f">>> Using sport '{sport}' for R resolver call for bet {bet_id}")
                logger.info(
                    f">>> This is a {'college' if sport in ['ncaab', 'ncaaf', 'ncaaw'] else 'professional'} game")
                # =========================================================

                # Clean player name for player props, but keep original for game markets
                cleaned_player = self._clean_player_name(bet.get('player', ''))
                full_player = bet.get('player', '')  # Original player string with line value
                market = bet.get('market', '')
                event = bet.get('event', '')
                game_date = bet.get('game_date')
                line_value = bet.get('line_value') or bet.get('market_line_value')
                direction = bet.get('direction') or bet.get('market_direction')
                stat_type = bet.get('market_stat_type')

                # Extract season from game date
                season = self._extract_season(game_date, sport)

                # Determine market type and call appropriate resolver method
                market_category = bet.get('market_category', '').lower()

                result = None

                if 'player_stat' in market_category:
                    # Player stat market - use cleaned player name
                    logger.info(f">>> Calling player_stat resolver for {cleaned_player} in sport {sport}")

                    # ========== FIX: Format market type for R ==========
                    formatted_market = self._format_market_type_for_r(market, sport, stat_type)
                    logger.info(f">>> Original market: '{market}' -> Formatted: '{formatted_market}'")

                    result = self.resolver.resolve_player_stat(
                        player_name=cleaned_player,
                        sport=sport,
                        season=season,
                        market_type=formatted_market,  # Use formatted version
                        event_string=event,
                        line_value=line_value,
                        game_date=game_date,
                        direction=direction,
                        stat_type=stat_type
                    )
                elif 'scorer' in market_category or 'first_last_scorer' in market_category:
                    # Scorer market - use cleaned player name
                    logger.info(f">>> Calling scorer resolver for {cleaned_player} in sport {sport}")
                    result = self.resolver.resolve_scorer(
                        player_name=cleaned_player,
                        sport=sport,
                        season=season,
                        scorer_type=market,
                        event_string=event,
                        game_date=game_date
                    )
                elif 'game' in market_category or 'total' in market_category or 'spread' in market_category:
                    # Game market - use FULL player string which contains team and line value
                    # Example: "Gonzaga -5.5" for a point spread bet
                    logger.info(
                        f"DEBUG: Game market - full_player='{full_player}', market='{market}', line_value={line_value}")

                    result = self.resolver.resolve_game_market(
                        event_string=event,
                        sport=sport,
                        season=season,
                        market_type=market,
                        line_value=line_value,
                        direction=direction,
                        team=full_player,  # Pass the full player string with team and line
                        game_date=game_date
                    )
                else:
                    # Generic fallback - try player stat resolver with cleaned name
                    logger.info(
                        f">>> Generic fallback - calling player_stat resolver for {cleaned_player} in sport {sport}")

                    # ========== FIX: Format market type for R even in fallback ==========
                    formatted_market = self._format_market_type_for_r(market, sport, stat_type)
                    logger.info(f">>> Original market: '{market}' -> Formatted: '{formatted_market}'")

                    result = self.resolver.resolve_player_stat(
                        player_name=cleaned_player,
                        sport=sport,
                        season=season,
                        market_type=formatted_market,  # Use formatted version
                        event_string=event,
                        line_value=line_value,
                        game_date=game_date,
                        direction=direction,
                        stat_type=stat_type
                    )

                if not result:
                    raise Exception("No result from resolver")

                # ========== NEW: Check for void conditions FIRST ==========
                # This catches "ESPN API returned no player stats" errors before any other processing
                error_msg = result.get('error', '')

                logger.info(f" DEBUG - Checking for void: error_msg='{error_msg}'")
                logger.info(f" DEBUG - Condition parts: error_msg exists={bool(error_msg)}")
                logger.info(
                    f" DEBUG - 'no player stats' in lower: {'no player stats' in error_msg.lower() if error_msg else False}")
                logger.info(
                    f" DEBUG - 'player not found' in lower: {'player not found' in error_msg.lower() if error_msg else False}")
                logger.info(f" DEBUG - 'no stats' in lower: {'no stats' in error_msg.lower() if error_msg else False}")

                if (error_msg and ('no player stats' in error_msg.lower() or
                                   'player not found' in error_msg.lower() or
                                   'no stats' in error_msg.lower())):
                    # Void the bet - refund stake
                    stake = float(bet.get('stake', 0))
                    profit = 0

                    logger.info(f"✅ Void condition detected from error: {error_msg}")

                    # Update bet in database as void
                    self.db.update_bet_result(bet_id, None, profit, status='void',
                                              void_reason='player_did_not_play')

                    self._update_bet_state(bet_id, self.STATES['VOID'],
                                           f"Bet voided: player_did_not_play")

                    self._audit('resolution_void', bet_id, resolution_id,
                                f"Bet voided: player_did_not_play - Stake returned: €{stake:.2f}")

                    # Return void result
                    return {
                        'success': True,
                        'resolved': True,
                        'won': None,
                        'void': True,
                        'void_reason': 'player_did_not_play',
                        'profit': 0,
                        'stake_returned': stake,
                        'state': 'void',
                        'error': None,
                        'message': f"VOID - player_did_not_play - Stake returned: €{stake:.2f}",
                        'resolution_id': resolution_id,
                        'timestamp': datetime.now().isoformat()
                    }

                # Also check data field for void indicators (even if success is True)
                if result.get('success'):
                    data = result.get('data', {})
                    if isinstance(data, dict):
                        if data.get('found') is False:
                            stake = float(bet.get('stake', 0))
                            profit = 0
                            logger.info(f"✅ Void condition detected: found=False in data")

                            self.db.update_bet_result(bet_id, None, profit, status='void',
                                                      void_reason='player_not_found')
                            self._update_bet_state(bet_id, self.STATES['VOID'],
                                                   f"Bet voided: player_not_found")

                            return {
                                'success': True,
                                'resolved': True,
                                'won': None,
                                'void': True,
                                'void_reason': 'player_not_found',
                                'profit': 0,
                                'stake_returned': stake,
                                'state': 'void',
                                'error': None,
                                'message': f"VOID - player_not_found - Stake returned: €{stake:.2f}",
                                'resolution_id': resolution_id,
                                'timestamp': datetime.now().isoformat()
                            }

                        # Check if player played 0 minutes
                        if 'stats' in data:
                            stats = data['stats']
                            if isinstance(stats, dict) and 'minutes' in stats and stats['minutes'] == 0:
                                stake = float(bet.get('stake', 0))
                                profit = 0
                                logger.info(f"✅ Void condition detected: player played 0 minutes")

                                self.db.update_bet_result(bet_id, None, profit, status='void',
                                                          void_reason='player_did_not_play')
                                self._update_bet_state(bet_id, self.STATES['VOID'],
                                                       f"Bet voided: player_did_not_play")

                                return {
                                    'success': True,
                                    'resolved': True,
                                    'won': None,
                                    'void': True,
                                    'void_reason': 'player_did_not_play',
                                    'profit': 0,
                                    'stake_returned': stake,
                                    'state': 'void',
                                    'error': None,
                                    'message': f"VOID - player_did_not_play - Stake returned: €{stake:.2f}",
                                    'resolution_id': resolution_id,
                                    'timestamp': datetime.now().isoformat()
                                }

                # Process successful resolution
                if result.get('success') and result.get('resolved'):
                    # Calculate profit
                    stake = float(bet.get('stake', 0))
                    odds = float(bet.get('odds', 0))

                    # Determine if won (and if it should be voided)
                    won, void_reason = self._determine_win(result, bet, line_value, direction)

                    if void_reason:
                        # Void the bet - refund stake
                        profit = 0  # No profit/loss, stake returned

                        # Update bet in database as void
                        self.db.update_bet_result(bet_id, None, profit, status='void',
                                                  void_reason=void_reason)

                        self._update_bet_state(bet_id, self.STATES['VOID'],
                                               f"Bet voided: {void_reason}")

                        self._audit('resolution_void', bet_id, resolution_id,
                                    f"Bet voided: {void_reason} - Stake returned: €{stake:.2f}")

                        # Return void result
                        return {
                            'success': True,
                            'resolved': True,
                            'won': None,
                            'void': True,
                            'void_reason': void_reason,
                            'profit': 0,
                            'stake_returned': stake,
                            'state': 'void',
                            'error': None,
                            'message': f"VOID - {void_reason} - Stake returned: €{stake:.2f}",
                            'resolution_id': resolution_id,
                            'timestamp': datetime.now().isoformat()
                        }
                    else:
                        # Normal win/loss
                        profit = stake * (odds - 1) if won else -stake

                        # Update bet in database
                        self.db.update_bet_result(bet_id, won, profit)

                        self._update_bet_state(bet_id, self.STATES['COMPLETE'],
                                               f"Resolved successfully: {'WIN' if won else 'LOSS'}")

                        self._audit('resolution_complete', bet_id, resolution_id,
                                    f"Bet resolved: {'WIN' if won else 'LOSS'} - Profit: {profit}")

                        # Return win/loss result
                        return {
                            'success': True,
                            'resolved': True,
                            'won': won,
                            'void': False,
                            'profit': profit,
                            'state': 'complete',
                            'error': None,
                            'message': f"{'WIN' if won else 'LOSS'} - Profit: €{profit:.2f}",
                            'resolution_id': resolution_id,
                            'timestamp': datetime.now().isoformat()
                        }
                else:
                    # Resolution attempted but not yet available
                    error_msg = result.get('error', 'Unknown error')

                    if self._is_retryable_error(error_msg):
                        # Schedule retry
                        next_retry = datetime.now() + timedelta(hours=self.resolver.retry_cooldown_hours)

                        self._update_bet_state(bet_id, self.STATES['RETRY_PENDING'],
                                               f"Retryable: {error_msg}")
                        self.db.update_bet_error(bet_id, error_msg)

                        self._audit('retry_scheduled', bet_id, resolution_id,
                                    f"Error: {error_msg}, Next retry: {next_retry.isoformat()}")

                        return {
                            'success': True,
                            'resolved': False,
                            'state': 'retry_pending',
                            'error': error_msg,
                            'next_retry': next_retry.isoformat(),
                            'message': f"Will retry: {error_msg}",
                            'resolution_id': resolution_id,
                            'timestamp': datetime.now().isoformat()
                        }
                    else:
                        # Permanent failure
                        self._update_bet_state(bet_id, self.STATES['FAILED_PERMANENT'],
                                               f"Permanent failure: {error_msg}")
                        self.db.update_bet_error(bet_id, error_msg)

                        self._audit('permanent_failure', bet_id, resolution_id,
                                    f"Permanent error: {error_msg}")

                        return {
                            'success': True,
                            'resolved': False,
                            'state': 'failed_permanent',
                            'error': error_msg,
                            'message': f"Failed permanently: {error_msg}",
                            'resolution_id': resolution_id,
                            'timestamp': datetime.now().isoformat()
                        }

            except Exception as e:
                error_msg = str(e)
                logger.error(f"Resolver exception: {error_msg}")

                if self._is_retryable_error(error_msg):
                    next_retry = datetime.now() + timedelta(hours=self.resolver.retry_cooldown_hours)
                    self._update_bet_state(bet_id, self.STATES['RETRY_PENDING'],
                                           f"Retryable error: {error_msg}")
                    self.db.update_bet_error(bet_id, error_msg)

                    return {
                        'success': True,
                        'resolved': False,
                        'state': 'retry_pending',
                        'error': error_msg,
                        'next_retry': next_retry.isoformat(),
                        'message': f"Will retry: {error_msg}",
                        'resolution_id': resolution_id,
                        'timestamp': datetime.now().isoformat()
                    }
                else:
                    self._update_bet_state(bet_id, self.STATES['FAILED_PERMANENT'],
                                           f"Permanent failure: {error_msg}")
                    self.db.update_bet_error(bet_id, error_msg)

                    return {
                        'success': True,
                        'resolved': False,
                        'state': 'failed_permanent',
                        'error': error_msg,
                        'message': f"Failed permanently: {error_msg}",
                        'resolution_id': resolution_id,
                        'timestamp': datetime.now().isoformat()
                    }

        except Exception as e:
            error_msg = f"Resolution manager error: {str(e)}"
            logger.error(f"Manager exception: {error_msg}")
            self._audit('manager_error', bet_id, resolution_id, error_msg)

            return {
                'success': False,
                'resolved': False,
                'state': 'failed_permanent',
                'error': error_msg,
                'message': f"Manager error: {error_msg}",
                'resolution_id': resolution_id,
                'timestamp': datetime.now().isoformat()
            }

    def _extract_season(self, game_date: str, sport: str) -> int:
        """Extract season from game date with support for all sports"""
        if not game_date:
            current_year = datetime.now().year
            current_month = datetime.now().month

            # Handle sports that span calendar years
            if sport in ["nfl", "ncaaf"] and current_month <= 2:
                return current_year - 1
            elif sport in ["nba", "nhl", "ncaab", "ncaaw"] and current_month <= 6:
                return current_year - 1
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

            # Default: use game year
            else:
                return game_year

        except Exception as e:
            logger.warning(f"Could not parse game date '{game_date}': {e}")
            current_year = datetime.now().year
            current_month = datetime.now().month

            if sport in ["nfl", "ncaaf"] and current_month <= 2:
                return current_year - 1
            elif sport in ["nba", "nhl", "ncaab", "ncaaw"] and current_month <= 6:
                return current_year - 1
            return current_year

    def _update_bet_state(self, bet_id: str, state: str, note: str = ""):
        """Update bet state with audit trail"""
        try:
            self.db.update_bet_state(bet_id, state, note)
        except Exception as e:
            logger.error(f"Error updating bet state: {e}")

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
        if len(self.audit_log) > 1000:
            self.audit_log = self.audit_log[-1000:]

    def _is_retryable_error(self, error_msg: str) -> bool:
        """Determine if an error is retryable"""
        if not error_msg:
            return False

        error_lower = error_msg.lower()
        retryable_phrases = [
            "not yet ready",
            "not resolved",
            "game not complete",
            "not available",
            "data not available",
            "schedule not found",
            "timeout",
            "temporarily unavailable",
            "no data found",
            "no game data",
            "game not started",
            "game in progress",
            "rate limit",
            "too many requests",
            "connection error",
            "could not find game id",  # For games that might be in the future
        ]

        return any(phrase in error_lower for phrase in retryable_phrases)

    def get_audit_trail(self, bet_id: str = None) -> List[Dict]:
        """Get audit trail for a specific bet or all bets"""
        if bet_id:
            return [entry for entry in self.audit_log if entry['bet_id'] == bet_id]
        return self.audit_log

    def batch_resolve(self, max_bets: int = 10, force: bool = False) -> Dict:
        """Batch resolution with rate limiting"""
        pending_bets = self.db.get_pending_bets(limit=max_bets)
        results = {
            'total': len(pending_bets),
            'complete': 0,
            'failed_permanent': 0,
            'retry_pending': 0,
            'void': 0,  # Added void count
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

            if result.get('state') == 'complete':
                results['complete'] += 1
            elif result.get('state') == 'failed_permanent':
                results['failed_permanent'] += 1
            elif result.get('state') == 'retry_pending':
                results['retry_pending'] += 1
            elif result.get('state') == 'void':
                results['void'] += 1
            else:
                results['manager_errors'] += 1

            time.sleep(0.5)

        return results