import sys
import os
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from message_queue import InMemoryQueue
from models import Transaction, TransactionType


def make_tx(client_id: int, tx_id: int) -> Transaction:
    return Transaction(
        tx_type=TransactionType.DEPOSIT,
        client_id=client_id,
        tx_id=tx_id,
        amount=Decimal("100"),
    )


class TestInMemoryQueue:
    def test_publish_consume(self):
        q = InMemoryQueue()
        tx = make_tx(1, 1)
        q.publish(tx)
        result = q.consume(timeout=0.1)
        assert result == tx

    def test_consume_empty_returns_none(self):
        q = InMemoryQueue()
        result = q.consume(timeout=0.1)
        assert result is None

    def test_is_empty(self):
        q = InMemoryQueue()
        assert q.is_empty()
        q.publish(make_tx(1, 1))
        assert not q.is_empty()
        q.consume(timeout=0.1)
        assert q.is_empty()

    def test_shutdown(self):
        q = InMemoryQueue()
        assert not q.is_shutdown()
        q.shutdown()
        assert q.is_shutdown()

    def test_dlq_send_and_get(self):
        q = InMemoryQueue()
        tx1 = make_tx(1, 1)
        tx2 = make_tx(2, 2)

        q.send_to_dlq(tx1)
        q.send_to_dlq(tx2)

        assert q.dlq_size() == 2

        messages = q.get_dlq_messages()
        assert len(messages) == 2
        assert tx1 in messages
        assert tx2 in messages

        assert q.dlq_size() == 0

    def test_dlq_empty(self):
        q = InMemoryQueue()
        messages = q.get_dlq_messages()
        assert messages == []
