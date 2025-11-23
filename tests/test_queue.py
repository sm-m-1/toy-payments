import sys
import os
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from message_queue import InMemoryQueue
from models import Transaction, TransactionType


def make_transaction(client_id: int, transaction_id: int) -> Transaction:
    return Transaction(
        transaction_type=TransactionType.DEPOSIT,
        client_id=client_id,
        transaction_id=transaction_id,
        amount=Decimal("100"),
    )


class TestInMemoryQueue:
    def test_publish_consume(self):
        queue = InMemoryQueue()
        transaction = make_transaction(1, 1)
        queue.publish_message(transaction)
        result = queue.consume_message()
        assert result == transaction

    def test_consume_empty_returns_none(self):
        queue = InMemoryQueue()
        result = queue.consume_message()
        assert result is None

    def test_is_empty(self):
        queue = InMemoryQueue()
        assert queue.is_empty()
        queue.publish_message(make_transaction(1, 1))
        assert not queue.is_empty()
        queue.consume_message()
        assert queue.is_empty()

    def test_shutdown(self):
        queue = InMemoryQueue()
        assert not queue.is_shutdown()
        queue.shutdown()
        assert queue.is_shutdown()

    def test_dead_letter_queue_send_and_get(self):
        queue = InMemoryQueue()
        transaction1 = make_transaction(1, 1)
        transaction2 = make_transaction(2, 2)

        queue.send_to_dead_letter_queue(transaction1)
        queue.send_to_dead_letter_queue(transaction2)

        assert queue.get_dead_letter_queue_size() == 2

        messages = queue.get_dead_letter_queue_messages()
        assert len(messages) == 2
        assert transaction1 in messages
        assert transaction2 in messages

        assert queue.get_dead_letter_queue_size() == 0

    def test_dead_letter_queue_empty(self):
        queue = InMemoryQueue()
        messages = queue.get_dead_letter_queue_messages()
        assert messages == []
