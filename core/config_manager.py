"""
Configuration Manager - Loads and manages all configuration settings
"""
import os
import json
import logging
from dataclasses import dataclass
from typing import Dict, Any

logger = logging.getLogger(__name__)

@dataclass
class DatabaseConfig:
    db_path: str = "../sports_betting.db"
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
    file: str = "../sports_betting.log"
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

                logger.info(f"Configuration loaded from {self.config_file}")
        except Exception as e:
            logger.warning(f"Could not load config file: {e}")

    def _update_from_dict(self, config_obj, config_dict: Dict[str, Any]):
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

        if self.database.use_supabase:
            try:
                from supabase import create_client
            except ImportError:
                issues.append("Supabase enabled but supabase package not installed. Run: pip install supabase")

        if self.database.use_supabase and not self.database.supabase_key:
            issues.append("SUPABASE_KEY is required when using Supabase")

        if self.bankroll.initial_bankroll <= 0:
            issues.append("Initial bankroll must be positive")

        if self.betting.min_ev_threshold < 0:
            issues.append("Minimum EV threshold cannot be negative")

        if issues:
            logger.error(f"Configuration validation failed: {', '.join(issues)}")
            return False

        return True

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary"""
        return {
            'database': self.database.__dict__,
            'api': self.api.__dict__,
            'bankroll': self.bankroll.__dict__,
            'scanner': self.scanner.__dict__,
            'betting': self.betting.__dict__,
            'logging': self.logging.__dict__,
        }

    def save(self, filepath: str = None):
        """Save configuration to file"""
        if filepath is None:
            filepath = self.config_file

        try:
            with open(filepath, 'w') as f:
                json.dump(self.to_dict(), f, indent=2)
            logger.info(f"Configuration saved to {filepath}")
        except Exception as e:
            logger.error(f"Could not save configuration: {e}")