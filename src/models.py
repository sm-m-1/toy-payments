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
