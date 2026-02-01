#!/usr/bin/env python3
"""
GitHub Actions Runner - Only runs Options 1 & 5
"""
import sys
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


def main():
    try:
        # Add current directory to path
        sys.path.insert(0, os.getcwd())

        logger.info('📦 Importing EnhancedPaperTradingSystem...')
        from enhanced_paper_trading import EnhancedPaperTradingSystem

        # ============================================
        # INITIALIZE SYSTEM
        # ============================================
        logger.info('🚀 Initializing system...')
        system = EnhancedPaperTradingSystem()
        logger.info('✅ System initialized successfully!')

        # ============================================
        # OPTION 1: Intelligent Automated Scanning
        # ============================================
        logger.info('')
        logger.info('=' * 50)
        logger.info('OPTION 1: Intelligent Automated Scanning')
        logger.info('=' * 50)

        scan_result = system.intelligent_automated_scanning()

        if scan_result:
            logger.info('📊 Scan Results:')
            logger.info(f'  • Bets placed: {scan_result.get("bets_placed", 0)}')
            logger.info(f'  • Total EV: {scan_result.get("total_ev", 0):.2f}')
            if scan_result.get('details'):
                logger.info(f'  • Details: {scan_result.get("details")}')
        else:
            logger.info('ℹ️ No bets placed in this scan')

        # ============================================
        # OPTION 5: Resolve Pending Bets
        # ============================================
        logger.info('')
        logger.info('=' * 50)
        logger.info('OPTION 5: Resolve Pending Bets')
        logger.info('=' * 50)

        resolve_result = system.resolve_pending_bets()

        if resolve_result:
            logger.info('📊 Resolution Results:')
            logger.info(f'  • Bets resolved: {resolve_result.get("resolved", 0)}')
            logger.info(f'  • Total profit: €{resolve_result.get("total_profit", 0):.2f}')
            if resolve_result.get('details'):
                logger.info(f'  • Details: {resolve_result.get("details")}')
        else:
            logger.info('ℹ️ No pending bets to resolve')

        # ============================================
        # FINAL STATISTICS
        # ============================================
        logger.info('')
        logger.info('=' * 50)
        logger.info('FINAL PERFORMANCE STATISTICS')
        logger.info('=' * 50)

        stats = system.get_performance_stats()
        logger.info(f'💰 Bankroll: €{stats.get("bankroll", 0):.2f}')
        logger.info(f'📈 Total bets: {stats.get("total_bets", 0)}')
        logger.info(f'✅ Won bets: {stats.get("won_bets", 0)}')
        logger.info(f'❌ Lost bets: {stats.get("lost_bets", 0)}')
        logger.info(f'⏳ Pending bets: {stats.get("pending_bets", 0)}')
        logger.info(f'📊 Win rate: {stats.get("win_rate", 0):.1f}%')

        logger.info('')
        logger.info('=' * 50)
        logger.info('✅ GITHUB ACTIONS RUN COMPLETED SUCCESSFULLY!')
        logger.info('=' * 50)

        return 0

    except ImportError as e:
        logger.error(f'❌ Import Error: {e}')
        logger.error('Make sure all dependencies are installed')
        return 1

    except Exception as e:
        logger.error(f'❌ Execution Error: {e}')
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())