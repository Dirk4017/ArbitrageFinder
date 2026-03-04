"""
SQLite Database Manager
"""
import sqlite3
import logging
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_path: str = "sports_betting.db"):
        self.db_path = db_path
        self.init_database()

    def init_database(self):
        """Initialize database with required tables"""
        conn = self.get_connection()
        try:
            # Bets table - UPDATED with market classification columns
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
                    resolution_note TEXT NULL,
                    market_category TEXT NULL,
                    market_subcategory TEXT NULL,
                    market_stat_type TEXT NULL,
                    market_line_value REAL NULL,
                    market_direction TEXT NULL,
                    market_period TEXT NULL
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

            # Add columns that may be missing from older databases
            for col_def in [
                ("void_reason", "TEXT NULL"),
                ("won", "INTEGER NULL"),
            ]:
                try:
                    conn.execute(f"ALTER TABLE bets ADD COLUMN {col_def[0]} {col_def[1]}")
                    conn.commit()
                    logger.info(f"Added missing column: {col_def[0]}")
                except sqlite3.OperationalError:
                    pass  # Column already exists

            logger.info("SQLite Database initialized successfully")

        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise
        finally:
            conn.close()

    def get_connection(self):
        """Get database connection with row factory"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def update_existing_bet_categories(self):
        """Update market classification for all existing bets"""
        from resolvers.market_classifier import MarketClassifier

        classifier = MarketClassifier()
        conn = self.get_connection()

        try:
            # Get all bets without categories
            cursor = conn.cursor()
            cursor.execute("SELECT id, market FROM bets WHERE market_category IS NULL")
            bets = cursor.fetchall()

            updated = 0
            for bet in bets:
                bet_id, market_text = bet
                classification = classifier.classify(market_text)

                cursor.execute('''
                    UPDATE bets 
                    SET market_category = ?, 
                        market_subcategory = ?,
                        market_stat_type = ?,
                        market_line_value = ?,
                        market_direction = ?,
                        market_period = ?
                    WHERE id = ?
                ''', (
                    classification.get('category'),
                    classification.get('subcategory'),
                    classification.get('stat'),
                    classification.get('line'),
                    classification.get('direction'),
                    classification.get('period'),
                    bet_id
                ))

                updated += 1

            conn.commit()
            logger.info(f"Updated {updated} bets with market classifications")
            return updated

        except Exception as e:
            logger.error(f"Error updating bet categories: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()

    def save_bet(self, bet_data: Dict) -> Optional[int]:
        """Save a new bet and return its ID"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO bets 
                (event, sport, market, player, odds, stake, potential_win, ev, sportsbook, 
                 status, placed_at, kelly_percent, edge_percent, game_date, resolution_state,
                 market_category, market_subcategory, market_stat_type, market_line_value,
                 market_direction, market_period)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                'pending',  # Initial resolution state
                bet_data.get('market_category'),
                bet_data.get('market_subcategory'),
                bet_data.get('market_stat_type'),
                bet_data.get('market_line_value'),
                bet_data.get('market_direction'),
                bet_data.get('market_period')
            ))
            bet_id = cursor.lastrowid
            conn.commit()
            logger.info(f"Bet saved with ID: {bet_id}")
            return bet_id
        except Exception as e:
            logger.error(f"Error saving bet: {e}")
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
            if row:
                return self._normalize_bet_data(dict(row))
            return None
        except Exception as e:
            logger.error(f"Error getting bet {bet_id}: {e}")
            return None
        finally:
            conn.close()

    def _normalize_bet_data(self, bet: Dict) -> Dict:
        """Normalize bet data from SQLite to consistent format"""
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

    def update_bet_result(self, bet_id: int, won: Optional[bool], profit: float,
                          status: str = 'complete', void_reason: Optional[str] = None):
        """Update bet result - supports win, loss, or void"""
        conn = self.get_connection()
        try:
            if status == 'void':
                conn.execute('''
                    UPDATE bets 
                    SET status = ?, result = ?, profit = ?, void_reason = ?, 
                        resolved_at = ?, resolution_state = ?, won = ?
                    WHERE id = ?
                ''', (
                    'void',
                    'void',
                    profit,  # Should be 0
                    void_reason,
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'void',
                    None,  # won is NULL for void bets
                    bet_id
                ))
            else:
                conn.execute('''
                    UPDATE bets 
                    SET status = ?, result = ?, profit = ?, resolved_at = ?, 
                        resolution_state = ?, won = ?
                    WHERE id = ?
                ''', (
                    'won' if won else 'lost',
                    'win' if won else 'loss',
                    profit,
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'complete',
                    won,
                    bet_id
                ))
            conn.commit()
            if status == 'void':
                logger.info(f"Bet {bet_id} voided: {void_reason}")
            else:
                logger.info(f"Bet {bet_id} updated: {'won' if won else 'lost'}")
        except Exception as e:
            logger.error(f"Error updating bet {bet_id}: {e}")
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
            logger.info(f"Updated bet {bet_id} error: {error[:50]}...")
        except Exception as e:
            logger.error(f"Error updating bet error: {e}")
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
            logger.info(f"Updated bet {bet_id} state to {state}: {note}")
        except Exception as e:
            logger.error(f"Error updating bet state: {e}")
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
            rows = cursor.fetchall()
            return [self._normalize_bet_data(dict(row)) for row in rows]
        finally:
            conn.close()

    def get_bet_history(self, limit: int = 100) -> List[Dict]:
        """Get bet history"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM bets ORDER BY placed_at DESC LIMIT ?", (limit,))
            rows = cursor.fetchall()
            return [self._normalize_bet_data(dict(row)) for row in rows]
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
        finally:
            conn.close()