"""
Complete Cleanup Script - Resolves all pending bets
Run this once to clear your pending bets
"""
import time
from enhanced_paper_trading import EnhancedPaperTradingSystem


def resolve_all_pending_bets():
    print("=" * 70)
    print("COMPLETE PENDING BETS RESOLUTION")
    print("=" * 70)

    # Initialize system
    system = EnhancedPaperTradingSystem()

    # Get all pending bets
    pending = system.db.get_pending_bets()
    print(f"\n📊 Total pending bets: {len(pending)}")

    # Statistics
    results = {
        'resolved_win': 0,
        'resolved_loss': 0,
        'future_skipped': 0,
        'college_skipped': 0,
        'failed': 0
    }

    print("\n" + "=" * 70)
    print("PROCESSING BETS")
    print("=" * 70)

    for i, bet in enumerate(pending, 1):
        bet_id = bet.get('id')
        event = bet.get('event', 'Unknown')
        sport = bet.get('sport', 'Unknown')
        market = bet.get('market', 'Unknown')
        player = bet.get('player', 'Unknown')
        game_date = bet.get('game_date', 'Unknown')
        stake = float(bet.get('stake', 0))

        print(f"\n[{i}/{len(pending)}] Processing: {player} - {market}")
        print(f"   Event: {event}")
        print(f"   Sport: {sport} | Date: {game_date} | Stake: €{stake:.2f}")

        # Check if it's a future game
        if game_date and game_date != 'None' and game_date != 'Unknown':
            try:
                from datetime import datetime
                game_dt = datetime.strptime(str(game_date), '%Y-%m-%d')
                if game_dt > datetime.now():
                    print(f"   ⏸ FUTURE GAME - Skipping (will resolve naturally)")
                    results['future_skipped'] += 1
                    continue
            except:
                pass

        # Try to resolve normally
        try:
            result = system.resolve_bet_intelligently(bet_id)

            if result.get('resolved'):
                won = result.get('won', False)
                profit = result.get('profit', 0)
                status = "✅ WIN" if won else "❌ LOSS"
                print(f"   {status} - Profit: €{profit:.2f}")

                if won:
                    results['resolved_win'] += 1
                else:
                    results['resolved_loss'] += 1

            elif result.get('skip_reason') == 'future_game':
                print(f"   ⏸ Future game - will resolve later")
                results['future_skipped'] += 1

            elif result.get('skip_reason') == 'college_game':
                print(f"   🎓 College game - forcing resolution as LOSS")
                # Force as loss
                system.db.update_bet_result(bet_id, False, -stake)
                results['resolved_loss'] += 1

            else:
                error = result.get('error', 'Unknown error')
                print(f"   ❌ Failed: {error}")

                # Force resolve based on error type
                if "Unknown market type" in error or "not found" in error or "Could not find game" in error:
                    print(f"   🔨 Force marking as LOSS")
                    system.db.update_bet_result(bet_id, False, -stake)
                    results['resolved_loss'] += 1
                else:
                    results['failed'] += 1

        except Exception as e:
            print(f"   ❌ Error: {e}")
            results['failed'] += 1

        time.sleep(0.5)  # Rate limiting

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"✅ Resolved as WIN:  {results['resolved_win']}")
    print(f"❌ Resolved as LOSS: {results['resolved_loss']}")
    print(f"⏸ Future games:      {results['future_skipped']}")
    print(f"❓ Failed:            {results['failed']}")
    print(f"📊 Total processed:   {len(pending)}")
    print("=" * 70)

    # Final check
    remaining = system.db.get_pending_bets()
    print(f"\n📊 Remaining pending bets: {len(remaining)}")

    if remaining:
        print("\n⚠️  Some bets still pending. These are likely future games.")
        for bet in remaining[:5]:  # Show first 5
            print(f"   • {bet.get('event')} - {bet.get('game_date')}")

    return results


def force_resolve_all_college_bets():
    """Force resolve all college basketball bets as losses"""
    print("\n" + "=" * 70)
    print("FORCE RESOLVING ALL COLLEGE BASKETBALL BETS")
    print("=" * 70)

    system = EnhancedPaperTradingSystem()
    pending = system.db.get_pending_bets()

    college_keywords = [
        'kansas state', 'tcu', 'oklahoma state', 'gonzaga', 'santa clara',
        'miami (fl)', 'nc state', 'pittsburgh', 'north carolina', 'iowa state',
        'florida atlantic', 'south florida', 'charleston', 'campbell',
        'mcneese state', 'east texas a&m', 'bethune-cookman', 'alcorn state',
        'unlv', 'boise state', 'byu', 'providence', 'villanova', 'umbc',
        'new hampshire', 'connecticut', 'virginia', 'clemson', 'wake forest',
        'duke', 'louisville', 'syracuse', 'boston college', 'purdue',
        'wisconsin', 'michigan state', 'illinois', 'northwestern'
    ]

    resolved = 0
    for bet in pending:
        event = bet.get('event', '').lower()
        sport = bet.get('sport', '').lower()
        bet_id = bet.get('id')
        stake = float(bet.get('stake', 0))

        if any(keyword in event for keyword in college_keywords) or sport in ['ncaab', 'ncaaf', 'ncaaw']:
            print(f"\nResolving: {bet.get('event')}")
            print(f"   Sport: {sport} | Stake: €{stake:.2f}")
            system.db.update_bet_result(bet_id, False, -stake)
            print(f"   ✅ Marked as LOSS (€{-stake:.2f})")
            resolved += 1
            time.sleep(0.3)

    print(f"\n✅ Force resolved {resolved} college bets as losses")
    return resolved


if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("BET RESOLUTION CLEANUP TOOL")
    print("=" * 70)
    print("\nOptions:")
    print("1. Resolve all pending bets intelligently")
    print("2. Force resolve all college bets as losses")
    print("3. Run complete cleanup (recommended)")

    choice = input("\nSelect option (1-3): ").strip()

    if choice == '1':
        resolve_all_pending_bets()
    elif choice == '2':
        force_resolve_all_college_bets()
    elif choice == '3':
        print("\n🔨 Running complete cleanup...")
        force_resolve_all_college_bets()
        time.sleep(1)
        resolve_all_pending_bets()
    else:
        print("Invalid choice")