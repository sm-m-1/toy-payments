import csv
import logging
import threading
from decimal import Decimal
from typing import Dict, Optional, List

from models import Transaction, TransactionType, ClientAccount, ProcessingResult
from message_queue import InMemoryQueue
from state import StateManager
from processor import TransactionProcessor

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

        # Phase 1: Main Processing (multi-threaded)
        logger.info("Starting main processing phase")

        publisher = threading.Thread(target=self._publish, args=(filepath,))
        publisher.start()

        consumers = []
        for _ in range(self._num_consumers):
            t = threading.Thread(target=self._consume)
            t.start()
            consumers.append(t)

        publisher.join()
        self._queue.shutdown()
        for t in consumers:
            t.join()

        logger.info("Main processing phase complete")

        # Phase 2: DLQ Retry (single-threaded)
        dlq_messages = self._queue.get_dlq_messages()
        if dlq_messages:
            logger.info(f"Retrying {len(dlq_messages)} messages from DLQ")
            self._process_dlq(dlq_messages)

        return self._state.get_all_accounts()

    def _publish(self, filepath: str) -> None:
        """Read CSV and publish transactions to queue."""
        with open(filepath, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                tx = self._parse_row(row)
                if tx:
                    self._queue.publish(tx)

    def _consume(self) -> None:
        """Consumer loop: pull from queue, process, send failures to DLQ."""
        while True:
            tx = self._queue.consume(timeout=0.1)
            if tx is None:
                if self._queue.is_shutdown() and self._queue.is_empty():
                    break
                continue

            lock = self._state.get_client_lock(tx.client_id)
            with lock:
                result = self._processor.process(tx)

            if result == ProcessingResult.FAILED_RETRIABLE:
                self._queue.send_to_dlq(tx)

    def _process_dlq(self, messages: List[Transaction]) -> None:
        """
        Process DLQ messages synchronously (single-threaded).
        This runs after main processing, so no race conditions.
        """
        still_failed = []

        for tx in messages:
            lock = self._state.get_client_lock(tx.client_id)
            with lock:
                result = self._processor.process(tx)

            if result == ProcessingResult.FAILED_RETRIABLE:
                still_failed.append(tx)
            elif result == ProcessingResult.FAILED_PERMANENT:
                logger.warning(f"DLQ message permanently failed: {tx}")

        if still_failed:
            logger.warning(f"{len(still_failed)} messages still failed after DLQ retry")
            for tx in still_failed:
                logger.warning(f"  Discarding: {tx}")

    def _parse_row(self, row: Dict[str, str]) -> Optional[Transaction]:
        """Parse CSV row into Transaction."""
        try:
            normalized = {k.strip(): v.strip() for k, v in row.items()}

            tx_type_str = normalized["type"].lower()
            client_id = int(normalized["client"])
            tx_id = int(normalized["tx"])

            amount = None
            amount_str = normalized.get("amount", "")
            if amount_str:
                amount = Decimal(amount_str)

            return Transaction(
                tx_type=TransactionType(tx_type_str),
                client_id=client_id,
                tx_id=tx_id,
                amount=amount,
            )
        except (KeyError, ValueError) as e:
            logger.warning(f"Failed to parse row {row}: {e}")
            return None
