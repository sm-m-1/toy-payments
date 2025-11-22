import queue
import threading
from typing import Optional, List

from models import Transaction


class InMemoryQueue:
    """
    Thread-safe message queue with Dead Letter Queue support.
    All synchronization is internal - callers never need to lock.
    """

    def __init__(self):
        self._main_queue: queue.Queue[Transaction] = queue.Queue()
        self._dlq: queue.Queue[Transaction] = queue.Queue()
        self._shutdown = threading.Event()

    def publish(self, message: Transaction) -> None:
        """Add message to main queue. Thread-safe."""
        self._main_queue.put(message)

    def consume(self, timeout: float = 0.1) -> Optional[Transaction]:
        """
        Get next message from main queue.
        Returns None if queue is empty after timeout.
        Thread-safe.
        """
        try:
            return self._main_queue.get(timeout=timeout)
        except queue.Empty:
            return None

    def is_empty(self) -> bool:
        """Check if main queue is empty."""
        return self._main_queue.empty()

    def send_to_dlq(self, message: Transaction) -> None:
        """Send failed message to DLQ for later retry. Thread-safe."""
        self._dlq.put(message)

    def get_dlq_messages(self) -> List[Transaction]:
        """
        Drain all messages from DLQ and return as list.
        Called after main processing is complete.
        """
        messages = []
        while True:
            try:
                messages.append(self._dlq.get_nowait())
            except queue.Empty:
                break
        return messages

    def dlq_size(self) -> int:
        """Return approximate DLQ size."""
        return self._dlq.qsize()

    def shutdown(self) -> None:
        """Signal no more messages will be published."""
        self._shutdown.set()

    def is_shutdown(self) -> bool:
        """Check if shutdown has been signaled."""
        return self._shutdown.is_set()
