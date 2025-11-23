import threading
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Optional


class TransactionType(Enum):
    DEPOSIT = "deposit"
    WITHDRAWAL = "withdrawal"
    DISPUTE = "dispute"
    RESOLVE = "resolve"
    CHARGEBACK = "chargeback"


class ProcessingResult(Enum):
    SUCCESS = "success"
    FAILED_RETRIABLE = "failed_retriable"
    FAILED_PERMANENT = "failed_permanent"


@dataclass
class Transaction:
    transaction_type: TransactionType
    client_id: int
    transaction_id: int
    amount: Optional[Decimal] = None

    def __repr__(self) -> str:
        return f"Transaction({self.transaction_type.value}, client={self.client_id}, tx={self.transaction_id}, amount={self.amount})"


@dataclass
class ClientAccount:
    client_id: int
    available: Decimal = Decimal("0")
    held: Decimal = Decimal("0")
    locked: bool = False

    @property
    def total(self) -> Decimal:
        return self.available + self.held

    def credit(self, amount: Decimal) -> None:
        self.available += amount

    def debit(self, amount: Decimal) -> None:
        self.available -= amount

    def hold(self, amount: Decimal) -> None:
        self.available -= amount
        self.held += amount

    def release_hold(self, amount: Decimal) -> None:
        self.held -= amount
        self.available += amount

    def remove_held(self, amount: Decimal) -> None:
        self.held -= amount


class ProcessingStats:
    """Thread-safe counters for tracking processing statistics."""

    def __init__(self):
        self._lock = threading.Lock()
        self.processed = 0
        self.failed = 0
        self.dlq_retried = 0

    def record_success(self):
        with self._lock:
            self.processed += 1

    def record_failure(self):
        with self._lock:
            self.failed += 1

    def record_dlq_retry(self):
        with self._lock:
            self.dlq_retried += 1
