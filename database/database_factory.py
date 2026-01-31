"""
Database Factory - Creates appropriate database manager based on configuration
"""
import logging
from typing import Optional

from supabase_manager import SupabaseManager
from .sqlite_manager import DatabaseManager

logger = logging.getLogger(__name__)


def create_database_manager(config):
    """Factory function to create the appropriate database manager"""
    if config.database.use_supabase:
        try:
            # Try to import supabase
            from supabase import create_client

            # Check if credentials are provided
            if not config.database.supabase_url or not config.database.supabase_key:
                logger.warning("Supabase credentials missing. Falling back to SQLite.")
                return DatabaseManager(config.database.db_path)

            # Try to create Supabase client
            supabase = create_client(config.database.supabase_url, config.database.supabase_key)

            # Test connection
            try:
                supabase.table('bets').select('count', count='exact').execute()
                logger.info("Successfully connected to Supabase")
                return SupabaseManager(config.database.supabase_url, config.database.supabase_key)
            except Exception as e:
                logger.warning(f"Failed to connect to Supabase: {e}. Falling back to SQLite.")
                return DatabaseManager(config.database.db_path)

        except ImportError:
            logger.warning("Supabase not installed. Falling back to SQLite.")
            return DatabaseManager(config.database.db_path)
        except Exception as e:
            logger.error(f"Unexpected error connecting to Supabase: {e}")
            return DatabaseManager(config.database.db_path)
    else:
        return DatabaseManager(config.database.db_path)