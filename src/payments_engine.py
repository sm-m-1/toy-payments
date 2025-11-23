import csv
import logging
import threading
from decimal import Decimal
from typing import Dict, Optional, List

from models import Transaction, TransactionType, ClientAccount, ProcessingResult
from message_queue import InMemoryQueue
from state_manager import StateManager
from transaction_processor import TransactionProcessor

logger = logging.getLogger(__name__)


class PaymentsEngine:
    """
    Orchestrates transaction processing with publisher-consumer pattern.
    Supports DLQ for handling out-of-order transactions.
    """

    def __init__(self, num_consumers: int = 4):
        self._num_consumers = num_consumers
        self._queue = InMemoryQueue()
        self._state = StateManager()
        self._processor = TransactionProcessor(self._state)

    def process_file(self, filepath: str) -> Dict[int, ClientAccount]:
        """Process CSV file and return final account states."""

        # Phase 1: Main Processing (1 publisher thread, N consumer threads)
        logger.info("Starting main processing phase")

        publisher_thread = threading.Thread(target=self._publish_transactions, args=(filepath,))
        publisher_thread.start()

        consumer_threads = []
        for _ in range(self._num_consumers):
            consumer_thread = threading.Thread(target=self._consume_transactions)
            consumer_thread.start()
            consumer_threads.append(consumer_thread)

        publisher_thread.join()
        self._queue.shutdown()
        for consumer_thread in consumer_threads:
            consumer_thread.join()

        logger.info("Main processing phase complete")

        # Phase 2: DLQ Retry (single-threaded)
        dead_letter_queue_messages = self._queue.get_dead_letter_queue_messages()
        if dead_letter_queue_messages:
            logger.info(f"Retrying {len(dead_letter_queue_messages)} messages from dead letter queue")
            self._process_dead_letter_queue(dead_letter_queue_messages)

        return self._state.get_all_accounts()

    def _publish_transactions(self, filepath: str) -> None:
        """Read CSV and publish transactions to queue."""
        with open(filepath, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                transaction = self._parse_csv_row(row)
                if transaction:
                    self._queue.publish_message(transaction)

    def _consume_transactions(self) -> None:
        """Consumer loop: pull from queue, process, send failures to DLQ."""
        while True:
            transaction = self._queue.consume_message()
            if transaction is None:
                if self._queue.is_shutdown() and self._queue.is_empty():
                    break
                continue

            lock = self._state.get_client_lock(transaction.client_id)
            with lock:
                result = self._processor.process_transaction(transaction)

            if result == ProcessingResult.FAILED_RETRIABLE:
                self._queue.send_to_dead_letter_queue(transaction)

    def _process_dead_letter_queue(self, messages: List[Transaction]) -> None:
        """
        Process dead letter queue messages synchronously (single-threaded).
        This runs after main processing, so no race conditions.
        """
        still_failed = []

        for transaction in messages:
            lock = self._state.get_client_lock(transaction.client_id)
            with lock:
                result = self._processor.process_transaction(transaction)

            if result == ProcessingResult.FAILED_RETRIABLE:
                still_failed.append(transaction)
            elif result == ProcessingResult.FAILED_PERMANENT:
                logger.warning(f"Dead letter queue message permanently failed: {transaction}")

        if still_failed:
            logger.warning(f"{len(still_failed)} messages still failed after dead letter queue retry")
            for transaction in still_failed:
                logger.warning(f"  Discarding: {transaction}")

    def _parse_csv_row(self, row: Dict[str, str]) -> Optional[Transaction]:
        """Parse CSV row into Transaction."""
        try:
            normalized = {k.strip(): v.strip() for k, v in row.items()}

            transaction_type_str = normalized["type"].lower()
            client_id = int(normalized["client"])
            transaction_id = int(normalized["tx"])

            amount = None
            amount_str = normalized.get("amount", "")
            if amount_str:
                amount = Decimal(amount_str)

            return Transaction(
                transaction_type=TransactionType(transaction_type_str),
                client_id=client_id,
                transaction_id=transaction_id,
                amount=amount,
            )
        except (KeyError, ValueError) as e:
            logger.warning(f"Failed to parse row {row}: {e}")
            return None
