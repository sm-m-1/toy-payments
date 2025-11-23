import logging

from models import Transaction, TransactionType, ClientAccount, ProcessingResult
from state_manager import StateManager

logger = logging.getLogger(__name__)


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
            case _:
                return ProcessingResult.FAILED_PERMANENT

    def _handle_deposit(self, account: ClientAccount, transaction: Transaction) -> ProcessingResult:
        if transaction.amount is None or transaction.amount <= 0:
            logger.warning(f"Deposit tx {transaction.transaction_id}: invalid amount {transaction.amount}")
            return ProcessingResult.FAILED_PERMANENT

        if self._state.get_transaction(transaction.transaction_id) is not None:
            logger.info(f"Deposit tx {transaction.transaction_id}: already processed, skipping (idempotent)")
            return ProcessingResult.SUCCESS

        account.credit(transaction.amount)
        self._state.store_transaction(transaction)
        return ProcessingResult.SUCCESS

    def _handle_withdrawal(self, account: ClientAccount, transaction: Transaction) -> ProcessingResult:
        if transaction.amount is None or transaction.amount <= 0:
            logger.warning(f"Withdrawal tx {transaction.transaction_id}: invalid amount {transaction.amount}")
            return ProcessingResult.FAILED_PERMANENT

        if self._state.get_transaction(transaction.transaction_id) is not None:
            logger.info(f"Withdrawal tx {transaction.transaction_id}: already processed, skipping (idempotent)")
            return ProcessingResult.SUCCESS

        if account.available >= transaction.amount:
            account.debit(transaction.amount)
            self._state.store_transaction(transaction)
            return ProcessingResult.SUCCESS
        return ProcessingResult.FAILED_PERMANENT

    def _handle_dispute(self, account: ClientAccount, transaction: Transaction) -> ProcessingResult:
        original = self._state.get_transaction(transaction.transaction_id)

        if original is None:
            logger.info(f"Dispute for tx {transaction.transaction_id}: transaction not found yet, likely out-of-order message delivery")
            return ProcessingResult.FAILED_RETRIABLE

        if original.client_id != transaction.client_id:
            logger.error(f"Dispute for tx {transaction.transaction_id}: client mismatch (expected {original.client_id}, got {transaction.client_id}). This should never happen.")
            return ProcessingResult.FAILED_PERMANENT

        if self._state.is_transaction_disputed(transaction.transaction_id):
            logger.warning(f"Dispute for tx {transaction.transaction_id}: transaction already disputed")
            return ProcessingResult.FAILED_PERMANENT

        # TODO: Withdrawal disputes (internal dispute resolution, fraud claims) could be supported by tracking payment state and attempting to recall funds
        if original.transaction_type != TransactionType.DEPOSIT:
            logger.warning(f"Dispute for tx {transaction.transaction_id}: only deposits can be disputed (got {original.transaction_type.value}), withdrawals not supported as funds already left account")
            return ProcessingResult.FAILED_PERMANENT

        account.hold(original.amount)
        self._state.mark_transaction_disputed(transaction.transaction_id)
        return ProcessingResult.SUCCESS

    def _handle_resolve(self, account: ClientAccount, transaction: Transaction) -> ProcessingResult:
        original = self._state.get_transaction(transaction.transaction_id)

        if original is None:
            return ProcessingResult.FAILED_RETRIABLE

        if not self._state.is_transaction_disputed(transaction.transaction_id):
            return ProcessingResult.FAILED_RETRIABLE

        account.release_hold(original.amount)
        self._state.clear_transaction_dispute(transaction.transaction_id)
        return ProcessingResult.SUCCESS

    def _handle_chargeback(self, account: ClientAccount, transaction: Transaction) -> ProcessingResult:
        original = self._state.get_transaction(transaction.transaction_id)

        if original is None:
            return ProcessingResult.FAILED_RETRIABLE

        if not self._state.is_transaction_disputed(transaction.transaction_id):
            return ProcessingResult.FAILED_RETRIABLE

        account.remove_held(original.amount)
        account.locked = True
        self._state.clear_transaction_dispute(transaction.transaction_id)
        return ProcessingResult.SUCCESS
