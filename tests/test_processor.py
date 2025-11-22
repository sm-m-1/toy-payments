import sys
import os
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from models import Transaction, TransactionType, ProcessingResult
from state import StateManager
from processor import TransactionProcessor


class TestTransactionProcessor:
    def setup_method(self):
        self.state = StateManager()
        self.processor = TransactionProcessor(self.state)

    def test_deposit(self):
        tx = Transaction(TransactionType.DEPOSIT, client_id=1, tx_id=1, amount=Decimal("100"))
        result = self.processor.process(tx)

        assert result == ProcessingResult.SUCCESS
        account = self.state.get_or_create_account(1)
        assert account.available == Decimal("100")
        assert account.total == Decimal("100")

    def test_withdrawal_success(self):
        deposit = Transaction(TransactionType.DEPOSIT, client_id=1, tx_id=1, amount=Decimal("100"))
        self.processor.process(deposit)

        withdrawal = Transaction(TransactionType.WITHDRAWAL, client_id=1, tx_id=2, amount=Decimal("60"))
        result = self.processor.process(withdrawal)

        assert result == ProcessingResult.SUCCESS
        account = self.state.get_or_create_account(1)
        assert account.available == Decimal("40")

    def test_withdrawal_insufficient_funds(self):
        deposit = Transaction(TransactionType.DEPOSIT, client_id=1, tx_id=1, amount=Decimal("50"))
        self.processor.process(deposit)

        withdrawal = Transaction(TransactionType.WITHDRAWAL, client_id=1, tx_id=2, amount=Decimal("100"))
        result = self.processor.process(withdrawal)

        assert result == ProcessingResult.FAILED_PERMANENT
        account = self.state.get_or_create_account(1)
        assert account.available == Decimal("50")

    def test_dispute(self):
        deposit = Transaction(TransactionType.DEPOSIT, client_id=1, tx_id=1, amount=Decimal("100"))
        self.processor.process(deposit)

        dispute = Transaction(TransactionType.DISPUTE, client_id=1, tx_id=1)
        result = self.processor.process(dispute)

        assert result == ProcessingResult.SUCCESS
        account = self.state.get_or_create_account(1)
        assert account.available == Decimal("0")
        assert account.held == Decimal("100")
        assert account.total == Decimal("100")

    def test_dispute_tx_not_found(self):
        dispute = Transaction(TransactionType.DISPUTE, client_id=1, tx_id=99)
        result = self.processor.process(dispute)
        assert result == ProcessingResult.FAILED_RETRIABLE

    def test_dispute_wrong_client(self):
        deposit = Transaction(TransactionType.DEPOSIT, client_id=1, tx_id=1, amount=Decimal("100"))
        self.processor.process(deposit)

        dispute = Transaction(TransactionType.DISPUTE, client_id=2, tx_id=1)
        result = self.processor.process(dispute)
        assert result == ProcessingResult.FAILED_PERMANENT

    def test_resolve(self):
        deposit = Transaction(TransactionType.DEPOSIT, client_id=1, tx_id=1, amount=Decimal("100"))
        self.processor.process(deposit)

        dispute = Transaction(TransactionType.DISPUTE, client_id=1, tx_id=1)
        self.processor.process(dispute)

        resolve = Transaction(TransactionType.RESOLVE, client_id=1, tx_id=1)
        result = self.processor.process(resolve)

        assert result == ProcessingResult.SUCCESS
        account = self.state.get_or_create_account(1)
        assert account.available == Decimal("100")
        assert account.held == Decimal("0")

    def test_resolve_not_disputed(self):
        deposit = Transaction(TransactionType.DEPOSIT, client_id=1, tx_id=1, amount=Decimal("100"))
        self.processor.process(deposit)

        resolve = Transaction(TransactionType.RESOLVE, client_id=1, tx_id=1)
        result = self.processor.process(resolve)
        assert result == ProcessingResult.FAILED_RETRIABLE

    def test_chargeback(self):
        deposit = Transaction(TransactionType.DEPOSIT, client_id=1, tx_id=1, amount=Decimal("100"))
        self.processor.process(deposit)

        dispute = Transaction(TransactionType.DISPUTE, client_id=1, tx_id=1)
        self.processor.process(dispute)

        chargeback = Transaction(TransactionType.CHARGEBACK, client_id=1, tx_id=1)
        result = self.processor.process(chargeback)

        assert result == ProcessingResult.SUCCESS
        account = self.state.get_or_create_account(1)
        assert account.available == Decimal("0")
        assert account.held == Decimal("0")
        assert account.total == Decimal("0")
        assert account.locked is True

    def test_frozen_account_rejects_operations(self):
        deposit = Transaction(TransactionType.DEPOSIT, client_id=1, tx_id=1, amount=Decimal("100"))
        self.processor.process(deposit)
        dispute = Transaction(TransactionType.DISPUTE, client_id=1, tx_id=1)
        self.processor.process(dispute)
        chargeback = Transaction(TransactionType.CHARGEBACK, client_id=1, tx_id=1)
        self.processor.process(chargeback)

        new_deposit = Transaction(TransactionType.DEPOSIT, client_id=1, tx_id=2, amount=Decimal("50"))
        result = self.processor.process(new_deposit)
        assert result == ProcessingResult.FAILED_PERMANENT
