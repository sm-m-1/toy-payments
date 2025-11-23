import sys
import os
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from payments_engine import PaymentsEngine

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


class TestPaymentsEngine:
    def test_basic_transactions(self):
        """Test from PDF example."""
        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(os.path.join(FIXTURES_DIR, "basic.csv"))

        assert 1 in accounts
        assert 2 in accounts

        assert accounts[1].available == Decimal("1.5")
        assert accounts[1].held == Decimal("0")
        assert accounts[1].total == Decimal("1.5")

        assert accounts[2].available == Decimal("2.0")
        assert accounts[2].held == Decimal("0")
        assert accounts[2].total == Decimal("2.0")

    def test_dispute_resolve(self):
        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(os.path.join(FIXTURES_DIR, "dispute_resolve.csv"))

        assert accounts[1].available == Decimal("100")
        assert accounts[1].held == Decimal("0")
        assert accounts[1].locked is False

    def test_chargeback(self):
        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(os.path.join(FIXTURES_DIR, "chargeback.csv"))

        assert accounts[1].available == Decimal("0")
        assert accounts[1].held == Decimal("0")
        assert accounts[1].total == Decimal("0")
        assert accounts[1].locked is True

    def test_out_of_order_dlq(self):
        """Dispute before deposit - should be handled by DLQ."""
        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(os.path.join(FIXTURES_DIR, "out_of_order.csv"))

        assert accounts[1].available == Decimal("0")
        assert accounts[1].held == Decimal("100")
        assert accounts[1].total == Decimal("100")

    def test_insufficient_funds(self):
        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(os.path.join(FIXTURES_DIR, "insufficient_funds.csv"))

        assert accounts[1].available == Decimal("50")
        assert accounts[1].total == Decimal("50")

    def test_decimal_precision(self):
        """Test 4 decimal place precision as per spec."""
        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(os.path.join(FIXTURES_DIR, "decimal_precision.csv"))

        # 1.2345 + 0.0001 - 0.2346 = 1.0000
        assert accounts[1].available == Decimal("1.0000")

    def test_dispute_withdrawal_ignored(self):
        """Disputing a withdrawal should be ignored."""
        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(os.path.join(FIXTURES_DIR, "dispute_withdrawal.csv"))

        # Deposit 100, withdraw 50, dispute withdrawal (ignored) = 50 available
        assert accounts[1].available == Decimal("50")
        assert accounts[1].held == Decimal("0")

    def test_duplicate_dispute_ignored(self):
        """Second dispute on same transaction should be ignored."""
        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(os.path.join(FIXTURES_DIR, "duplicate_dispute.csv"))

        # Deposit 100, dispute (held=100), duplicate dispute (ignored)
        assert accounts[1].available == Decimal("0")
        assert accounts[1].held == Decimal("100")

    def test_frozen_account_rejects_operations(self):
        """Operations after chargeback should be rejected."""
        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(os.path.join(FIXTURES_DIR, "frozen_account_operations.csv"))

        # Deposit 100, dispute, chargeback (frozen), deposit 50 (rejected), withdrawal (rejected)
        assert accounts[1].available == Decimal("0")
        assert accounts[1].total == Decimal("0")
        assert accounts[1].locked is True

    def test_wrong_client_dispute_ignored(self):
        """Client cannot dispute another client's transaction."""
        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(os.path.join(FIXTURES_DIR, "wrong_client_dispute.csv"))

        # Client 1 deposits 100, Client 2 tries to dispute (ignored)
        assert accounts[1].available == Decimal("100")
        assert accounts[1].held == Decimal("0")

    def test_resolve_without_dispute_ignored(self):
        """Resolve on non-disputed transaction should be ignored."""
        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(os.path.join(FIXTURES_DIR, "resolve_without_dispute.csv"))

        assert accounts[1].available == Decimal("100")
        assert accounts[1].held == Decimal("0")

    def test_chargeback_after_resolve_ignored(self):
        """Chargeback after resolve should be ignored (dispute already resolved)."""
        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(os.path.join(FIXTURES_DIR, "chargeback_after_resolve.csv"))

        # Deposit 100, dispute, resolve (back to available), chargeback (ignored)
        assert accounts[1].available == Decimal("100")
        assert accounts[1].held == Decimal("0")
        assert accounts[1].locked is False

    def test_partial_withdrawal_then_dispute(self):
        """Dispute after partial withdrawal holds full deposit amount."""
        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(os.path.join(FIXTURES_DIR, "partial_withdrawal_then_dispute.csv"))

        # Deposit 100, withdraw 30, dispute deposit (hold 100)
        # available = 70 - 100 = -30, held = 100
        assert accounts[1].available == Decimal("-30")
        assert accounts[1].held == Decimal("100")
        assert accounts[1].total == Decimal("70")

    def test_multiple_disputes_same_client(self):
        """Multiple disputes on different transactions, one resolved, one charged back."""
        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(os.path.join(FIXTURES_DIR, "multiple_disputes_same_client.csv"))

        # Deposit 100, deposit 50, dispute tx1, dispute tx2, resolve tx1, chargeback tx2
        # After resolve tx1: available=100, held=50
        # After chargeback tx2: available=100, held=0, total=100, locked=True
        assert accounts[1].available == Decimal("100")
        assert accounts[1].held == Decimal("0")
        assert accounts[1].total == Decimal("100")
        assert accounts[1].locked is True

    def test_redispute_after_resolve(self):
        """Transaction can be re-disputed after resolve, then charged back."""
        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(os.path.join(FIXTURES_DIR, "redispute_after_resolve.csv"))

        # Deposit 100, dispute, resolve, dispute again, chargeback
        assert accounts[1].available == Decimal("0")
        assert accounts[1].held == Decimal("0")
        assert accounts[1].total == Decimal("0")
        assert accounts[1].locked is True
