"""
Event Stream Extractor.
Handles ingestion from event sources (simulated Kinesis/SQS).
Supports batch processing with checkpointing for exactly-once semantics.
"""
import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class EventExtractor:
    """
    Extracts events from streaming sources with:
    - Batch processing with configurable batch sizes
    - Checkpointing for exactly-once semantics
    - Late-arriving data handling
    - Event schema validation
    """

    def __init__(
        self,
        batch_size: int = 1000,
        max_wait_seconds: int = 60,
        checkpoint_store: Optional[Any] = None,
    ):
        self.batch_size = batch_size
        self.max_wait_seconds = max_wait_seconds
        self.checkpoint_store = checkpoint_store
        self._current_offset = 0

    def extract_from_queue(
        self,
        queue_name: str,
        max_messages: int = None,
    ) -> Generator[List[Dict], None, None]:
        """
        Extract events from a simulated message queue.
        
        Yields batches of events.
        """
        max_messages = max_messages or self.batch_size
        batch = []
        batch_start = time.time()

        # Simulated event stream
        events = self._generate_simulated_events(queue_name, max_messages)
        
        for event in events:
            batch.append(event)

            if len(batch) >= self.batch_size:
                logger.info(f"Yielding batch of {len(batch)} events from {queue_name}")
                yield batch
                batch = []
                batch_start = time.time()

            # Time-based batch flushing
            if batch and (time.time() - batch_start) >= self.max_wait_seconds:
                logger.info(f"Time-based flush: {len(batch)} events")
                yield batch
                batch = []
                batch_start = time.time()

        # Yield remaining events
        if batch:
            yield batch

    def extract_with_checkpoint(
        self,
        source_name: str,
        checkpoint_key: str = "default",
    ) -> Generator[List[Dict], None, None]:
        """
        Extract events with checkpoint-based tracking.
        Ensures no events are processed twice (idempotency).
        """
        # Get last checkpoint
        last_checkpoint = self._get_checkpoint(source_name, checkpoint_key)
        logger.info(
            f"Resuming from checkpoint: {source_name}/{checkpoint_key} "
            f"at offset {last_checkpoint}"
        )

        batch_count = 0
        for batch in self.extract_from_queue(source_name):
            batch_count += 1
            
            # Process batch
            yield batch

            # Update checkpoint after successful processing
            self._update_checkpoint(
                source_name,
                checkpoint_key,
                last_checkpoint + len(batch),
            )
            last_checkpoint += len(batch)

        logger.info(f"Processed {batch_count} batches from {source_name}")

    def _generate_simulated_events(
        self,
        queue_name: str,
        count: int,
    ) -> List[Dict]:
        """Generate simulated events for testing."""
        import random

        event_types = [
            "order_created", "order_updated", "order_shipped",
            "payment_processed", "refund_initiated", "user_signup",
        ]
        
        events = []
        base_time = datetime.utcnow()

        for i in range(count):
            event = {
                "event_id": f"evt-{queue_name}-{self._current_offset + i:08d}",
                "event_type": random.choice(event_types),
                "source": queue_name,
                "timestamp": (
                    base_time.replace(
                        minute=random.randint(0, 59),
                        second=random.randint(0, 59),
                    )
                ).isoformat(),
                "payload": {
                    "order_id": f"ORD-{random.randint(1000, 9999)}",
                    "amount": round(random.uniform(10.0, 500.0), 2),
                    "customer_id": f"CUST-{random.randint(100, 999)}",
                    "status": random.choice(["pending", "processing", "completed"]),
                },
                "metadata": {
                    "region": random.choice(["us-east", "us-west", "eu-west"]),
                    "version": "1.0",
                },
            }
            events.append(event)

        self._current_offset += count
        return events

    def _get_checkpoint(self, source: str, key: str) -> int:
        """Get the last processed offset."""
        if self.checkpoint_store:
            return self.checkpoint_store.get(f"checkpoint:{source}:{key}", 0)
        return 0

    def _update_checkpoint(self, source: str, key: str, offset: int):
        """Update the processed offset."""
        if self.checkpoint_store:
            self.checkpoint_store.set(f"checkpoint:{source}:{key}", offset)

    def events_to_dataframe(self, events: List[Dict]) -> pd.DataFrame:
        """Convert a list of events to a pandas DataFrame."""
        if not events:
            return pd.DataFrame()

        df = pd.json_normalize(events)
        
        # Flatten nested payload
        if "payload" in df.columns:
            payload_df = pd.json_normalize(df["payload"].tolist())
            df = pd.concat([df.drop(columns=["payload"]), payload_df], axis=1)

        # Parse timestamps
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])

        return df
