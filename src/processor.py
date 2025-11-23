from models import Transaction, TransactionType, ClientAccount, ProcessingResult
from state import StateManager


class TransactionProcessor:
    """
    Processes transactions against state.
    Returns ProcessingResult to indicate success/failure type.
    Caller is responsible for holding appropriate client lock.
    """

    def __init__(self, state: StateManager):
        self._state = state

    def process_transaction(self, transaction: Transaction) -> ProcessingResult:
        """
        Process a single transaction.

        Returns:
            SUCCESS: Processed successfully
            FAILED_RETRIABLE: May succeed if retried later (e.g., transaction not found yet)
            FAILED_PERMANENT: Will never succeed (e.g., wrong client, frozen account)
        """
        account = self._state.get_or_create_account(transaction.client_id)

        if account.locked:
            return ProcessingResult.FAILED_PERMANENT

        match transaction.transaction_type:
            case TransactionType.DEPOSIT:
                return self._handle_deposit(account, transaction)
            case TransactionType.WITHDRAWAL:
                return self._handle_withdrawal(account, transaction)
            case TransactionType.DISPUTE:
                return self._handle_dispute(account, transaction)
            case TransactionType.RESOLVE:
                return self._handle_resolve(account, transaction)
            case TransactionType.CHARGEBACK:
                return self._handle_chargeback(account, transaction)

        return ProcessingResult.FAILED_PERMANENT

    def _handle_deposit(self, account: ClientAccount, transaction: Transaction) -> ProcessingResult:
        account.available += transaction.amount
        self._state.store_transaction(transaction)
        return ProcessingResult.SUCCESS

    def _handle_withdrawal(self, account: ClientAccount, transaction: Transaction) -> ProcessingResult:
        if account.available >= transaction.amount:
            account.available -= transaction.amount
            self._state.store_transaction(transaction)
            return ProcessingResult.SUCCESS
        return ProcessingResult.FAILED_PERMANENT

    def _handle_dispute(self, account: ClientAccount, transaction: Transaction) -> ProcessingResult:
        original = self._state.get_transaction(transaction.transaction_id)

        if original is None:
            return ProcessingResult.FAILED_RETRIABLE

        if original.client_id != transaction.client_id:
            return ProcessingResult.FAILED_PERMANENT

        if self._state.is_transaction_disputed(transaction.transaction_id):
            return ProcessingResult.FAILED_PERMANENT

        if original.transaction_type != TransactionType.DEPOSIT:
            return ProcessingResult.FAILED_PERMANENT

        account.available -= original.amount
        account.held += original.amount
        self._state.mark_transaction_disputed(transaction.transaction_id)
        return ProcessingResult.SUCCESS

    def _handle_resolve(self, account: ClientAccount, transaction: Transaction) -> ProcessingResult:
        original = self._state.get_transaction(transaction.transaction_id)

        if original is None:
            return ProcessingResult.FAILED_RETRIABLE

        if not self._state.is_transaction_disputed(transaction.transaction_id):
            return ProcessingResult.FAILED_RETRIABLE

        account.held -= original.amount
        account.available += original.amount
        self._state.clear_transaction_dispute(transaction.transaction_id)
        return ProcessingResult.SUCCESS

    def _handle_chargeback(self, account: ClientAccount, transaction: Transaction) -> ProcessingResult:
        original = self._state.get_transaction(transaction.transaction_id)

        if original is None:
            return ProcessingResult.FAILED_RETRIABLE

        if not self._state.is_transaction_disputed(transaction.transaction_id):
            return ProcessingResult.FAILED_RETRIABLE

        account.held -= original.amount
        account.locked = True
        self._state.clear_transaction_dispute(transaction.transaction_id)
        return ProcessingResult.SUCCESS
