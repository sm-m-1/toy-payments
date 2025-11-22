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

    def process(self, tx: Transaction) -> ProcessingResult:
        """
        Process a single transaction.

        Returns:
            SUCCESS: Processed successfully
            FAILED_RETRIABLE: May succeed if retried later (e.g., tx not found yet)
            FAILED_PERMANENT: Will never succeed (e.g., wrong client, frozen account)
        """
        account = self._state.get_or_create_account(tx.client_id)

        if account.locked:
            return ProcessingResult.FAILED_PERMANENT

        match tx.tx_type:
            case TransactionType.DEPOSIT:
                return self._handle_deposit(account, tx)
            case TransactionType.WITHDRAWAL:
                return self._handle_withdrawal(account, tx)
            case TransactionType.DISPUTE:
                return self._handle_dispute(account, tx)
            case TransactionType.RESOLVE:
                return self._handle_resolve(account, tx)
            case TransactionType.CHARGEBACK:
                return self._handle_chargeback(account, tx)

        return ProcessingResult.FAILED_PERMANENT

    def _handle_deposit(self, account: ClientAccount, tx: Transaction) -> ProcessingResult:
        account.available += tx.amount
        self._state.store_transaction(tx)
        return ProcessingResult.SUCCESS

    def _handle_withdrawal(self, account: ClientAccount, tx: Transaction) -> ProcessingResult:
        if account.available >= tx.amount:
            account.available -= tx.amount
            self._state.store_transaction(tx)
            return ProcessingResult.SUCCESS
        return ProcessingResult.FAILED_PERMANENT

    def _handle_dispute(self, account: ClientAccount, tx: Transaction) -> ProcessingResult:
        original = self._state.get_transaction(tx.tx_id)

        if original is None:
            return ProcessingResult.FAILED_RETRIABLE

        if original.client_id != tx.client_id:
            return ProcessingResult.FAILED_PERMANENT

        if self._state.is_disputed(tx.tx_id):
            return ProcessingResult.FAILED_PERMANENT

        if original.tx_type != TransactionType.DEPOSIT:
            return ProcessingResult.FAILED_PERMANENT

        account.available -= original.amount
        account.held += original.amount
        self._state.mark_disputed(tx.tx_id)
        return ProcessingResult.SUCCESS

    def _handle_resolve(self, account: ClientAccount, tx: Transaction) -> ProcessingResult:
        original = self._state.get_transaction(tx.tx_id)

        if original is None:
            return ProcessingResult.FAILED_RETRIABLE

        if not self._state.is_disputed(tx.tx_id):
            return ProcessingResult.FAILED_RETRIABLE

        account.held -= original.amount
        account.available += original.amount
        self._state.clear_dispute(tx.tx_id)
        return ProcessingResult.SUCCESS

    def _handle_chargeback(self, account: ClientAccount, tx: Transaction) -> ProcessingResult:
        original = self._state.get_transaction(tx.tx_id)

        if original is None:
            return ProcessingResult.FAILED_RETRIABLE

        if not self._state.is_disputed(tx.tx_id):
            return ProcessingResult.FAILED_RETRIABLE

        account.held -= original.amount
        account.locked = True
        self._state.clear_dispute(tx.tx_id)
        return ProcessingResult.SUCCESS
