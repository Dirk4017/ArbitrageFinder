#!/usr/bin/env python3
"""
Enhanced Sports Betting Paper Trading System
Main entry point - orchestrates all components
"""

import sys
import os
import logging

# Add the current directory to path so we can import our modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from enhanced_paper_trading import EnhancedPaperTradingSystem

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sports_betting.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def main():
    """Main entry point"""
    print("=" * 60)
    print("ENHANCED SPORTS BETTING PAPER TRADING SYSTEM")
    print("=" * 60)

    try:
        # Initialize the system
        system = EnhancedPaperTradingSystem()

        # Run the main menu
        system.run()

    except KeyboardInterrupt:
        print("\n\nExiting...")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        print(f"\nFatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()