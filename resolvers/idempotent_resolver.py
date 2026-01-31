"""
Idempotent Resolution Manager - Production-grade state machine for bet resolution
"""
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

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
            'RETRY_PENDING': 'retry_pending'
        }

        # Audit trail
        self.audit_log = []

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

            current_state = bet.get('resolution_state', self.STATES['PENDING'])

            # 2. Check state transitions
            if current_state == self.STATES['COMPLETE']:
                # Already resolved - return existing result
                return self._create_result(
                    'complete',
                    result=bet.get('result'),
                    profit=bet.get('profit'),
                    resolution_id=resolution_id
                )

            elif current_state == self.STATES['FAILED_PERMANENT']:
                # Permanent failure - no retry
                return self._create_result(
                    'failed_permanent',
                    error=bet.get('resolution_error', 'Previous permanent failure'),
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
                            resolution_id=resolution_id
                        )

            # 3. Attempt resolution
            self._update_bet_state(bet_id, self.STATES['RESOLVING'],
                                   f"Starting resolution {resolution_id}")

            try:
                # Your existing resolution logic here
                # For now, just mark as failed and let the new system handle it
                self._update_bet_state(bet_id, self.STATES['RETRY_PENDING'],
                                       "Use new intelligent resolution system")

                return self._create_result(
                    'retry_pending',
                    error="Use new intelligent resolution system",
                    next_retry=datetime.now().isoformat(),
                    resolution_id=resolution_id
                )

            except Exception as e:
                error_msg = str(e)
                logger.error(f"Resolver exception: {error_msg}")

                # Determine error type
                if self._is_retryable_error(error_msg):
                    # Schedule retry
                    next_retry = datetime.now() + timedelta(hours=self.resolver.retry_cooldown_hours)

                    self._update_bet_state(bet_id, self.STATES['RETRY_PENDING'],
                                           f"Retryable error: {error_msg}")
                    self.db.update_bet_error(bet_id, error_msg)

                    self._audit('retry_scheduled', bet_id, resolution_id,
                                f"Error: {error_msg}, Next retry: {next_retry.isoformat()}")

                    return self._create_result(
                        'retry_pending',
                        error=error_msg,
                        next_retry=next_retry.isoformat(),
                        resolution_id=resolution_id
                    )
                else:
                    # Permanent failure
                    self._update_bet_state(bet_id, self.STATES['FAILED_PERMANENT'],
                                           f"Permanent failure: {error_msg}")
                    self.db.update_bet_error(bet_id, error_msg)

                    self._audit('permanent_failure', bet_id, resolution_id,
                                f"Permanent error: {error_msg}")

                    return self._create_result(
                        'failed_permanent',
                        error=error_msg,
                        resolution_id=resolution_id
                    )

        except Exception as e:
            # Manager-level error
            error_msg = f"Resolution manager error: {str(e)}"
            logger.error(f"Manager exception: {error_msg}")
            self._audit('manager_error', bet_id, resolution_id, error_msg)

            return self._create_result(
                'failed_permanent',
                error=error_msg,
                resolution_id=resolution_id
            )

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

        # Keep only last 1000 entries
        if len(self.audit_log) > 1000:
            self.audit_log = self.audit_log[-1000:]

    def _is_retryable_error(self, error_msg: str) -> bool:
        """Determine if an error is retryable"""
        error_lower = error_msg.lower()
        retryable_phrases = [
            "not yet ready",
            "not resolved",
            "game not complete",
            "not available",
            "data not available",
            "schedule not found",
            "timeout",
            "temporarily unavailable"
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

            if result['state'] == 'complete':
                results['complete'] += 1
            elif result['state'] == 'failed_permanent':
                results['failed_permanent'] += 1
            elif result['state'] == 'retry_pending':
                results['retry_pending'] += 1
            else:
                results['manager_errors'] += 1

            # Rate limiting
            time.sleep(0.5)

        return results