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
        self._disputed: Set[int] = set()

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

    def store_transaction(self, tx: Transaction) -> None:
        """Store transaction for future dispute lookups."""
        self._transactions[tx.tx_id] = tx

    def get_transaction(self, tx_id: int) -> Optional[Transaction]:
        """Retrieve stored transaction by ID."""
        return self._transactions.get(tx_id)

    def mark_disputed(self, tx_id: int) -> None:
        """Mark a transaction as disputed."""
        self._disputed.add(tx_id)

    def is_disputed(self, tx_id: int) -> bool:
        """Check if transaction is currently disputed."""
        return tx_id in self._disputed

    def clear_dispute(self, tx_id: int) -> None:
        """Clear dispute status for a transaction."""
        self._disputed.discard(tx_id)

    def get_all_accounts(self) -> Dict[int, ClientAccount]:
        """Return all accounts (for final output)."""
        return dict(self._accounts)
