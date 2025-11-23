import sys
import os
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from models import Transaction, TransactionType, ClientAccount, ProcessingResult


class TestTransaction:
    def test_create_deposit(self):
        transaction = Transaction(
            transaction_type=TransactionType.DEPOSIT,
            client_id=1,
            transaction_id=1,
            amount=Decimal("100.0"),
        )
        assert transaction.transaction_type == TransactionType.DEPOSIT
        assert transaction.client_id == 1
        assert transaction.transaction_id == 1
        assert transaction.amount == Decimal("100.0")

    def test_create_dispute_no_amount(self):
        transaction = Transaction(
            transaction_type=TransactionType.DISPUTE,
            client_id=1,
            transaction_id=1,
        )
        assert transaction.amount is None


class TestClientAccount:
    def test_default_values(self):
        account = ClientAccount(client_id=1)
        assert account.available == Decimal("0")
        assert account.held == Decimal("0")
        assert account.locked is False

    def test_total_property(self):
        account = ClientAccount(
            client_id=1,
            available=Decimal("100"),
            held=Decimal("50"),
        )
        assert account.total == Decimal("150")


class TestProcessingResult:
    def test_enum_values(self):
        assert ProcessingResult.SUCCESS.value == "success"
        assert ProcessingResult.FAILED_RETRIABLE.value == "failed_retriable"
        assert ProcessingResult.FAILED_PERMANENT.value == "failed_permanent"
