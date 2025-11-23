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
