import sys
import os
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from payments_engine import PaymentsEngine


class TestPaymentsEngine:
    def test_basic_transactions(self, tmp_path):
        """Test from PDF example."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text('\n'.join([
            "type, client, tx, amount",
            "deposit, 1, 1, 1.0",
            "deposit, 2, 2, 2.0",
            "deposit, 1, 3, 2.0",
            "withdrawal, 1, 4, 1.5",
            "withdrawal, 2, 5, 3.0",
        ]))

        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(str(csv_file))

        assert 1 in accounts
        assert 2 in accounts

        assert accounts[1].available == Decimal("1.5")
        assert accounts[1].held == Decimal("0")
        assert accounts[1].total == Decimal("1.5")

        assert accounts[2].available == Decimal("2.0")
        assert accounts[2].held == Decimal("0")
        assert accounts[2].total == Decimal("2.0")

    def test_dispute_resolve(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text('\n'.join([
            "type, client, tx, amount",
            "deposit, 1, 1, 100.0",
            "dispute, 1, 1,",
            "resolve, 1, 1,",
        ]))

        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(str(csv_file))

        assert accounts[1].available == Decimal("100")
        assert accounts[1].held == Decimal("0")
        assert accounts[1].locked is False

    def test_chargeback(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text('\n'.join([
            "type, client, tx, amount",
            "deposit, 1, 1, 100.0",
            "dispute, 1, 1,",
            "chargeback, 1, 1,",
        ]))

        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(str(csv_file))

        assert accounts[1].available == Decimal("0")
        assert accounts[1].held == Decimal("0")
        assert accounts[1].total == Decimal("0")
        assert accounts[1].locked is True

    def test_out_of_order_dlq(self, tmp_path):
        """Dispute before deposit - should be handled by DLQ."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text('\n'.join([
            "type, client, tx, amount",
            "dispute, 1, 1,",
            "deposit, 1, 1, 100.0",
        ]))

        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(str(csv_file))

        assert accounts[1].available == Decimal("0")
        assert accounts[1].held == Decimal("100")
        assert accounts[1].total == Decimal("100")

    def test_insufficient_funds(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text('\n'.join([
            "type, client, tx, amount",
            "deposit, 1, 1, 50.0",
            "withdrawal, 1, 2, 100.0",
        ]))

        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(str(csv_file))

        assert accounts[1].available == Decimal("50")
        assert accounts[1].total == Decimal("50")

    def test_decimal_precision(self, tmp_path):
        """Test 4 decimal place precision as per spec."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text('\n'.join([
            "type, client, tx, amount",
            "deposit, 1, 1, 1.2345",
            "deposit, 1, 2, 0.0001",
            "withdrawal, 1, 3, 0.2346",
        ]))

        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(str(csv_file))

        # 1.2345 + 0.0001 - 0.2346 = 1.0000
        assert accounts[1].available == Decimal("1.0000")

    def test_dispute_withdrawal_ignored(self, tmp_path):
        """Disputing a withdrawal should be ignored."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text('\n'.join([
            "type, client, tx, amount",
            "deposit, 1, 1, 100.0",
            "withdrawal, 1, 2, 50.0",
            "dispute, 1, 2,",
        ]))

        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(str(csv_file))

        # Deposit 100, withdraw 50, dispute withdrawal (ignored) = 50 available
        assert accounts[1].available == Decimal("50")
        assert accounts[1].held == Decimal("0")

    def test_duplicate_dispute_ignored(self, tmp_path):
        """Second dispute on same transaction should be ignored."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text('\n'.join([
            "type, client, tx, amount",
            "deposit, 1, 1, 100.0",
            "dispute, 1, 1,",
            "dispute, 1, 1,",
        ]))

        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(str(csv_file))

        # Deposit 100, dispute (held=100), duplicate dispute (ignored)
        assert accounts[1].available == Decimal("0")
        assert accounts[1].held == Decimal("100")

    def test_frozen_account_rejects_operations(self, tmp_path):
        """Operations after chargeback should be rejected."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text('\n'.join([
            "type, client, tx, amount",
            "deposit, 1, 1, 100.0",
            "dispute, 1, 1,",
            "chargeback, 1, 1,",
            "deposit, 1, 2, 50.0",
            "withdrawal, 1, 3, 10.0",
        ]))

        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(str(csv_file))

        # Deposit 100, dispute, chargeback (frozen), deposit 50 (rejected), withdrawal (rejected)
        assert accounts[1].available == Decimal("0")
        assert accounts[1].total == Decimal("0")
        assert accounts[1].locked is True

    def test_wrong_client_dispute_ignored(self, tmp_path):
        """Client cannot dispute another client's transaction."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text('\n'.join([
            "type, client, tx, amount",
            "deposit, 1, 1, 100.0",
            "dispute, 2, 1,",
        ]))

        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(str(csv_file))

        # Client 1 deposits 100, Client 2 tries to dispute (ignored)
        assert accounts[1].available == Decimal("100")
        assert accounts[1].held == Decimal("0")

    def test_resolve_without_dispute_ignored(self, tmp_path):
        """Resolve on non-disputed transaction should be ignored."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text('\n'.join([
            "type, client, tx, amount",
            "deposit, 1, 1, 100.0",
            "resolve, 1, 1,",
        ]))

        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(str(csv_file))

        assert accounts[1].available == Decimal("100")
        assert accounts[1].held == Decimal("0")

    def test_chargeback_after_resolve_ignored(self, tmp_path):
        """Chargeback after resolve should be ignored (dispute already resolved)."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text('\n'.join([
            "type, client, tx, amount",
            "deposit, 1, 1, 100.0",
            "dispute, 1, 1,",
            "resolve, 1, 1,",
            "chargeback, 1, 1,",
        ]))

        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(str(csv_file))

        # Deposit 100, dispute, resolve (back to available), chargeback (ignored)
        assert accounts[1].available == Decimal("100")
        assert accounts[1].held == Decimal("0")
        assert accounts[1].locked is False

    def test_partial_withdrawal_then_dispute(self, tmp_path):
        """Dispute after partial withdrawal holds full deposit amount."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text('\n'.join([
            "type, client, tx, amount",
            "deposit, 1, 1, 100.0",
            "withdrawal, 1, 2, 30.0",
            "dispute, 1, 1,",
        ]))

        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(str(csv_file))

        # Deposit 100, withdraw 30, dispute deposit (hold 100)
        # available = 70 - 100 = -30, held = 100
        assert accounts[1].available == Decimal("-30")
        assert accounts[1].held == Decimal("100")
        assert accounts[1].total == Decimal("70")

    def test_multiple_disputes_same_client(self, tmp_path):
        """Multiple disputes on different transactions, one resolved, one charged back."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text('\n'.join([
            "type, client, tx, amount",
            "deposit, 1, 1, 100.0",
            "deposit, 1, 2, 50.0",
            "dispute, 1, 1,",
            "dispute, 1, 2,",
            "resolve, 1, 1,",
            "chargeback, 1, 2,",
        ]))

        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(str(csv_file))

        # Deposit 100, deposit 50, dispute tx1, dispute tx2, resolve tx1, chargeback tx2
        # After resolve tx1: available=100, held=50
        # After chargeback tx2: available=100, held=0, total=100, locked=True
        assert accounts[1].available == Decimal("100")
        assert accounts[1].held == Decimal("0")
        assert accounts[1].total == Decimal("100")
        assert accounts[1].locked is True

    def test_redispute_after_resolve(self, tmp_path):
        """Transaction can be re-disputed after resolve, then charged back."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text('\n'.join([
            "type, client, tx, amount",
            "deposit, 1, 1, 100.0",
            "dispute, 1, 1,",
            "resolve, 1, 1,",
            "dispute, 1, 1,",
            "chargeback, 1, 1,",
        ]))

        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(str(csv_file))

        # Deposit 100, dispute, resolve, dispute again, chargeback
        assert accounts[1].available == Decimal("0")
        assert accounts[1].held == Decimal("0")
        assert accounts[1].total == Decimal("0")
        assert accounts[1].locked is True

    def test_negative_deposit_rejected(self, tmp_path):
        """Deposit with negative amount should be rejected."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text('\n'.join([
            "type, client, tx, amount",
            "deposit, 1, 1, -100.0",
            "deposit, 1, 2, 50.0",
        ]))

        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(str(csv_file))

        # Negative deposit rejected, only 50 deposited
        assert accounts[1].available == Decimal("50")
        assert accounts[1].total == Decimal("50")

    def test_zero_deposit_rejected(self, tmp_path):
        """Deposit with zero amount should be rejected."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text('\n'.join([
            "type, client, tx, amount",
            "deposit, 1, 1, 0",
            "deposit, 1, 2, 100.0",
        ]))

        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(str(csv_file))

        # Zero deposit rejected, only 100 deposited
        assert accounts[1].available == Decimal("100")

    def test_negative_withdrawal_rejected(self, tmp_path):
        """Withdrawal with negative amount should be rejected."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text('\n'.join([
            "type, client, tx, amount",
            "deposit, 1, 1, 100.0",
            "withdrawal, 1, 2, -50.0",
        ]))

        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(str(csv_file))

        # Negative withdrawal rejected, balance unchanged
        assert accounts[1].available == Decimal("100")

    def test_duplicate_deposit_idempotent(self, tmp_path):
        """Duplicate deposit with same tx_id should be skipped (idempotent)."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text('\n'.join([
            "type, client, tx, amount",
            "deposit, 1, 1, 100.0",
            "deposit, 1, 1, 100.0",
            "deposit, 1, 1, 100.0",
        ]))

        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(str(csv_file))

        # Only first deposit processed, duplicates skipped
        assert accounts[1].available == Decimal("100")

    def test_duplicate_withdrawal_idempotent(self, tmp_path):
        """Duplicate withdrawal with same tx_id should be skipped (idempotent)."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text('\n'.join([
            "type, client, tx, amount",
            "deposit, 1, 1, 200.0",
            "withdrawal, 1, 2, 50.0",
            "withdrawal, 1, 2, 50.0",
            "withdrawal, 1, 2, 50.0",
        ]))

        engine = PaymentsEngine(num_consumers=2)
        accounts = engine.process_file(str(csv_file))

        # Deposit 200, only first withdrawal processed (50), duplicates skipped
        assert accounts[1].available == Decimal("150")
