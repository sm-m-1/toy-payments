import threading
from queue import Queue, Empty
from typing import Optional, List

from models import Transaction


class InMemoryQueue:
    """
    Thread-safe message queue with Dead Letter Queue support.
    All synchronization is internal - callers never need to lock.
    """

    DEFAULT_TIMEOUT = 0.1

    def __init__(self):
        self._main_queue: Queue[Transaction] = Queue()
        self._dead_letter_queue: Queue[Transaction] = Queue()
        self._shutdown_event = threading.Event()

    def publish_message(self, message: Transaction) -> None:
        """Add message to main queue. Thread-safe."""
        self._main_queue.put(message)

    def consume_message(self) -> Optional[Transaction]:
        """
        Get next message from main queue.
        Returns None if queue is empty after timeout.
        Thread-safe.
        """
        try:
            return self._main_queue.get(timeout=self.DEFAULT_TIMEOUT)
        except Empty:
            return None

    def is_empty(self) -> bool:
        """Check if main queue is empty."""
        return self._main_queue.empty()

    def send_to_dead_letter_queue(self, message: Transaction) -> None:
        """Send failed message to dead letter queue for later retry. Thread-safe."""
        self._dead_letter_queue.put(message)

    def get_dead_letter_queue_messages(self) -> List[Transaction]:
        """
        Drain all messages from dead letter queue and return as list.
        Called after main processing is complete.
        """
        messages = []
        while True:
            try:
                messages.append(self._dead_letter_queue.get(timeout=self.DEFAULT_TIMEOUT))
            except Empty:
                break
        return messages

    def get_dead_letter_queue_size(self) -> int:
        """Return approximate dead letter queue size."""
        return self._dead_letter_queue.qsize()

    def shutdown(self) -> None:
        """Signal no more messages will be published."""
        self._shutdown_event.set()

    def is_shutdown(self) -> bool:
        """Check if shutdown has been signaled."""
        return self._shutdown_event.is_set()
