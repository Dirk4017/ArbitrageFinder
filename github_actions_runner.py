#!/usr/bin/env python3
"""
GitHub Actions Runner - Runs automated scanning and resolves pending bets
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
        # OPTION 1: Automated Scanning
        # ============================================
        logger.info('')
        logger.info('=' * 50)
        logger.info('OPTION 1: Automated Scanning')
        logger.info('=' * 50)
        
        # Run automated scanning (2 scans)
        system.automated_scanning(max_scans=2)
        
        # ============================================
        # OPTION 5: Resolve Pending Bets
        # ============================================
        logger.info('')
        logger.info('=' * 50)
        logger.info('OPTION 5: Resolve Pending Bets')
        logger.info('=' * 50)
        
        # Get pending bets count before
        pending_before = len(system.db.get_pending_bets())
        logger.info(f'Pending bets before: {pending_before}')
        
        if pending_before > 0:
            # Resolve pending bets
            system._resolve_pending()
        else:
            logger.info('ℹ️ No pending bets to resolve')
        
        # Get pending bets count after
        pending_after = len(system.db.get_pending_bets())
        logger.info(f'Pending bets after: {pending_after}')
        
        # ============================================
        # FINAL STATISTICS
        # ============================================
        logger.info('')
        logger.info('=' * 50)
        logger.info('FINAL PERFORMANCE STATISTICS')
        logger.info('=' * 50)
        
        stats = system.db.get_performance_stats()
        logger.info(f'💰 Bankroll: €{system.bankroll:.2f}')
        logger.info(f'📈 Total bets: {stats.get("total_bets", 0)}')
        logger.info(f'✅ Won bets: {stats.get("won_bets", 0)}')
        logger.info(f'❌ Lost bets: {stats.get("lost_bets", 0)}')
        logger.info(f'⏳ Pending bets: {pending_after}')
        if stats.get('total_bets', 0) > 0:
            win_rate = (stats.get('won_bets', 0) / stats.get('total_bets', 1)) * 100
            logger.info(f'📊 Win rate: {win_rate:.1f}%')
        else:
            logger.info('📊 Win rate: 0.0%')
        
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
