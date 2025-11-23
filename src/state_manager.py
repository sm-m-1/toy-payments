import threading
from typing import Dict, Optional, Set

from models import Transaction, ClientAccount


class StateManager:
    """
    Thread-safe state management with per-client locking.
    Stores client accounts and transaction history for dispute lookups.
    """

    def __init__(self):
        self._accounts: Dict[int, ClientAccount] = {}
        self._transactions: Dict[int, Transaction] = {}
        self._disputed_transaction_ids: Set[int] = set()

        # Global lock protects creation of new entries in _accounts and _client_locks dicts.
        # Without it, two threads could create duplicate locks for the same client.
        # A database like Postgres would handle this internally via row-level locking.
        self._global_lock = threading.Lock()
        self._client_locks: Dict[int, threading.Lock] = {}

    def get_client_lock(self, client_id: int) -> threading.Lock:
        """
        Get or create a lock for a specific client.
        Consumer acquires this before processing any transaction for that client.
        """
        with self._global_lock:
            if client_id not in self._client_locks:
                self._client_locks[client_id] = threading.Lock()
            return self._client_locks[client_id]

    def get_or_create_account(self, client_id: int) -> ClientAccount:
        """Get existing account or create new one."""
        with self._global_lock:
            if client_id not in self._accounts:
                self._accounts[client_id] = ClientAccount(client_id=client_id)
            return self._accounts[client_id]

    def store_transaction(self, transaction: Transaction) -> None:
        """Store transaction for future dispute lookups."""
        self._transactions[transaction.transaction_id] = transaction

    def get_transaction(self, transaction_id: int) -> Optional[Transaction]:
        """Retrieve stored transaction by ID."""
        return self._transactions.get(transaction_id)

    def mark_transaction_disputed(self, transaction_id: int) -> None:
        """Mark a transaction as disputed."""
        self._disputed_transaction_ids.add(transaction_id)

    def is_transaction_disputed(self, transaction_id: int) -> bool:
        """Check if transaction is currently disputed."""
        return transaction_id in self._disputed_transaction_ids

    def clear_transaction_dispute(self, transaction_id: int) -> None:
        """Clear dispute status for a transaction."""
        self._disputed_transaction_ids.discard(transaction_id)

    def get_all_accounts(self) -> Dict[int, ClientAccount]:
        """Return all accounts (for final output)."""
        return dict(self._accounts)
