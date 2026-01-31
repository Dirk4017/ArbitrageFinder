"""
Supabase Database Manager
"""
import uuid
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any

try:
    from supabase import create_client, Client

    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False

logger = logging.getLogger(__name__)


class SupabaseManager:
    def __init__(self, supabase_url: str, supabase_key: str):
        if not SUPABASE_AVAILABLE:
            raise ImportError("Supabase not available. Install with: pip install supabase")

        self.supabase_url = supabase_url
        self.supabase_key = supabase_key
        self.supabase: Client = create_client(supabase_url, supabase_key)

        # Test connection
        try:
            response = self.supabase.table('bets').select('id').limit(1).execute()
            logger.info("Successfully connected to Supabase")
        except Exception as e:
            logger.error(f"Failed to connect to Supabase: {e}")
            raise

    def save_bet(self, bet_data: Dict) -> Optional[str]:
        """Save a new bet to Supabase"""
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
            'game_date': bet_data.get('game_date'),
            'resolution_state': 'pending',
            'resolution_error': None,
            'last_resolution_attempt': None,
            'market_category': bet_data.get('market_category'),
            'market_subcategory': bet_data.get('market_subcategory'),
            'market_stat_type': bet_data.get('market_stat_type'),
            'market_line_value': bet_data.get('market_line_value'),
            'market_direction': bet_data.get('market_direction'),
            'market_period': bet_data.get('market_period')
        }

        try:
            response = self.supabase.table('bets').insert(bet_record).execute()
            if hasattr(response, 'error') and response.error:
                logger.error(f"Error saving bet: {response.error}")
                return None
            logger.info(f"Bet saved to Supabase: {bet_data['event']}")
            return bet_id
        except Exception as e:
            logger.error(f"Exception saving bet: {e}")
            return None

    def get_bet(self, bet_id: str) -> Optional[Dict]:
        """Get a single bet by ID"""
        try:
            response = self.supabase.table('bets').select('*').eq('id', bet_id).execute()

            if response.data and len(response.data) > 0:
                bet = response.data[0]
                return self._normalize_bet_data(bet)
            return None
        except Exception as e:
            logger.error(f"Error getting bet: {e}")
            return None

    def _normalize_bet_data(self, bet: Dict) -> Dict:
        """Normalize bet data from Supabase to consistent format"""
        return {
            'id': bet['id'],
            'player': bet.get('player', ''),
            'player_name': bet.get('player', ''),
            'sport': bet.get('sport', ''),
            'market': bet.get('market', ''),
            'market_type': bet.get('market', ''),
            'event': bet.get('event', ''),
            'event_string': bet.get('event', ''),
            'game_date': bet.get('game_date'),
            'stake': float(bet.get('stake', 0)),
            'odds': float(bet.get('odds', 0)),
            'line_value': None,
            'season': None,
            'status': bet.get('status', 'pending'),
            'profit': float(bet.get('profit', 0)),
            'result': bet.get('result'),
            'market_category': bet.get('market_category'),
            'market_subcategory': bet.get('market_subcategory'),
            'market_stat_type': bet.get('market_stat_type'),
            'market_line_value': bet.get('market_line_value'),
            'market_direction': bet.get('market_direction'),
            'market_period': bet.get('market_period')
        }

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
            logger.info(f"Bet {bet_id} updated: {'won' if won else 'lost'} (€{profit:.2f})")
        except Exception as e:
            logger.error(f"Error updating bet: {e}")

    def update_existing_bet_categories(self):
        """Update market classification for all existing bets"""
        from resolvers.market_classifier import MarketClassifier

        classifier = MarketClassifier()

        try:
            # Get bets without categories
            response = self.supabase.table("bets").select("*").is_("market_category", "null").execute()
            bets = response.data

            if not bets:
                logger.info("No bets found without categories in Supabase")
                print("✓ No bets need updating - all have categories!")
                return 0

            print(f"Found {len(bets)} bets without categories")
            updated = 0

            for i, bet in enumerate(bets):
                bet_id = bet['id']
                market_text = bet.get('market', '')
                player_text = bet.get('player', '')

                if not market_text:
                    continue

                print(f"\n[{i + 1}/{len(bets)}] Processing: {player_text} - {market_text}")

                try:
                    # This returns a MarketClassification OBJECT, not a dict
                    classification = classifier.classify(market_text, player_text)
                    # Safely extract attributes with defaults
                    category = getattr(classification, 'category', None)
                    subcategory = getattr(classification, 'subcategory', None)
                    stat_type = getattr(classification, 'stat_type', None)  # ← Note: stat_type not stat
                    line_value = getattr(classification, 'line', None)
                    direction = getattr(classification, 'direction', None)
                    period = getattr(classification, 'period', None)

                    print(f"  Extracted: category={category}, subcategory={subcategory}")

                    # Update in Supabase
                    update_data = {}
                    if category:
                        update_data['market_category'] = category
                    if subcategory:
                        update_data['market_subcategory'] = subcategory
                    if stat_type:
                        update_data['market_stat_type'] = stat_type
                    if line_value is not None:
                        update_data['market_line_value'] = line_value
                    if direction:
                        update_data['market_direction'] = direction
                    if period:
                        update_data['market_period'] = period

                    if update_data:
                        self.supabase.table("bets").update(update_data).eq("id", bet_id).execute()
                        logger.info(f"Updated bet {bet_id}: {category}")
                        updated += 1
                        print(f"  ✓ Updated with: {category}")
                    else:
                        print(f"  ✗ No data to update")

                except Exception as e:
                    logger.error(f"Error classifying bet {bet_id}: {e}")
                    print(f"  ✗ Error: {e}")
                    continue

            logger.info(f"Updated {updated} bets with market classifications in Supabase")
            print(f"\n✓ Updated {updated} bets with missing categories")
            return updated

        except Exception as e:
            logger.error(f"Error updating bet categories in Supabase: {e}")
            print(f"✗ Error: {e}")
            return 0


    def update_bet_error(self, bet_id: str, error: str):
        """Update bet with error information"""
        update_data = {
            'resolution_error': error,
            'last_resolution_attempt': datetime.now().isoformat()
        }

        try:
            response = self.supabase.table('bets').update(update_data).eq('id', bet_id).execute()
            logger.info(f"Updated bet {bet_id} error: {error[:50]}...")
        except Exception as e:
            logger.error(f"Error updating bet error: {e}")

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
            logger.info(f"Updated bet {bet_id} state to {state}: {note}")
        except Exception as e:
            logger.error(f"Error updating bet state: {e}")

    def get_pending_bets(self, limit: int = None) -> List[Dict]:
        """Get all pending bets from Supabase"""
        try:
            query = self.supabase.table('bets').select('*').eq('status', 'pending')
            if limit:
                query = query.limit(limit)
            response = query.execute()

            if response.data:
                return [self._normalize_bet_data(bet) for bet in response.data]
            return []
        except Exception as e:
            logger.error(f"Error fetching pending bets: {e}")
            return []

    def get_bet_history(self, limit: int = 100) -> List[Dict]:
        """Get bet history from Supabase"""
        try:
            response = self.supabase.table('bets').select('*').order('placed_at', desc=True).limit(limit).execute()
            if response.data:
                return [self._normalize_bet_data(bet) for bet in response.data]
            return []
        except Exception as e:
            logger.error(f"Error fetching bet history: {e}")
            return []

    def get_performance_stats(self) -> Dict:
        """Get comprehensive performance statistics from Supabase"""
        try:
            response = self.supabase.table('bets').select('*').execute()
            all_bets = response.data if response.data else []

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
            logger.error(f"Error getting performance stats: {e}")
            return {
                'total_bets': 0,
                'won_bets': 0,
                'lost_bets': 0,
                'pending_bets': 0,
                'total_profit': 0,
                'total_wagered': 0,
                'win_rate': 0
            }