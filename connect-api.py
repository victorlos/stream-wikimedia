"""Stream Wikimedia recent changes into a Kafka topic.

Reads the public Server-Sent Events feed of every edit made to a Wikimedia
project and produces each event, unchanged, to Kafka.

Configuration comes from environment variables so the same script runs on a
laptop now and in a container on AWS later:

    WIKIMEDIA_CONTACT        required: your email or a URL (Wikimedia rejects
                             clients that do not identify themselves)
    KAFKA_BOOTSTRAP_SERVERS  default localhost:9092
    KAFKA_TOPIC              default wikimedia-recentchange
    STREAM_URL               default https://stream.wikimedia.org/v2/stream/recentchange
"""

import json
import logging
import os
import signal
import sys
import time
from dotenv import load_dotenv

import requests
from confluent_kafka import KafkaException, Producer

load_dotenv()

STREAM_URL = os.environ.get("STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "wikimedia-recentchange")
CONTACT = os.environ.get("WIKIMEDIA_CONTACT", "").strip()
STATS_INTERVAL_SECONDS = 10
MAX_BACKOFF_SECONDS = 30

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("producer")

stats = {"sent": 0, "skipped": 0, "delivered": 0, "failed": 0}
running = True


def stop(signum, _frame):
    """Finish the current event, then shut down cleanly."""
    global running
    log.info("Signal %s received, shutting down", signum)
    running = False


def iter_sse(lines):
    """Turn the lines of a Server-Sent Events response into (event_id, data) pairs.

    An event is a block of "field: value" lines ended by a blank line. Only the
    "id" and "data" fields matter here. Lines starting with ":" are keep-alive
    comments.
    """
    event_id = None
    data = []
    for raw in lines:
        line = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        if line == "":
            if data:
                yield event_id, "\n".join(data)
            data = []
            continue
        if line.startswith(":"):
            continue
        field, _, value = line.partition(":")
        value = value.removeprefix(" ")
        if field == "data":
            data.append(value)
        elif field == "id":
            event_id = value


def on_delivery(err, _msg):
    """Called by the Kafka client once the broker has confirmed or rejected a message."""
    if err is None:
        stats["delivered"] += 1
    else:
        stats["failed"] += 1
        log.error("Delivery failed: %s", err)


def send(producer, key, value):
    """Hand one message to the Kafka client, waiting if its local queue is full."""
    while True:
        try:
            producer.produce(TOPIC, key=key, value=value, on_delivery=on_delivery)
            break
        except BufferError:
            producer.poll(1)
    producer.poll(0)  # serve delivery callbacks


def handle(producer, data):
    """Validate one event and produce it."""
    try:
        event = json.loads(data)
    except json.JSONDecodeError:
        stats["skipped"] += 1
        log.warning("Skipping an event that is not valid JSON")
        return

    # Wikimedia sends synthetic "canary" events to test its own pipeline.
    if event.get("meta", {}).get("domain") == "canary":
        stats["skipped"] += 1
        return

    # The key decides the partition. Keying by wiki ("enwiki", "wikidatawiki",
    # ...) keeps all the edits of one wiki in order on one partition.
    key = event.get("wiki", "unknown")
    send(producer, key.encode("utf-8"), data.encode("utf-8"))
    stats["sent"] += 1


def run(producer):
    headers = {
        "Accept": "text/event-stream",
        "User-Agent": f"wiki-streaming-learning-project/0.1 ({CONTACT})",
    }
    last_event_id = None
    backoff = 1
    next_stats = time.monotonic() + STATS_INTERVAL_SECONDS

    while running:
        # After a dropped connection, ask the server to resume where we stopped.
        if last_event_id:
            headers["Last-Event-ID"] = last_event_id
        try:
            with requests.get(STREAM_URL, headers=headers, stream=True, timeout=(10, 60)) as response:
                response.raise_for_status()
                log.info("Connected to %s", STREAM_URL)
                for event_id, data in iter_sse(response.iter_lines()):
                    handle(producer, data)
                    last_event_id = event_id or last_event_id
                    backoff = 1
                    if time.monotonic() >= next_stats:
                        log.info("Stats: %s", stats)
                        next_stats = time.monotonic() + STATS_INTERVAL_SECONDS
                    if not running:
                        break
            if running:
                log.info("Stream closed by the server, reconnecting")
                time.sleep(1)
        except requests.RequestException as exc:
            log.warning("Stream error: %s. Reconnecting in %ss", exc, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)


def main():
    if not CONTACT:
        sys.exit(
            "Set WIKIMEDIA_CONTACT to your email or a URL first, for example:\n"
            "  export WIKIMEDIA_CONTACT=you@example.com"
        )

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "client.id": "wikimedia-producer",
            # Retries can never write a message twice or out of order.
            # This also sets acks=all.
            "enable.idempotence": True,
            # Wait up to 50 ms to fill a batch, then compress the whole batch.
            "linger.ms": 50,
            "compression.type": "lz4",
        }
    )

    try:
        # Fail fast with a clear message if the broker is not up.
        producer.list_topics(timeout=10)
    except KafkaException as exc:
        sys.exit(f"Cannot reach Kafka at {BOOTSTRAP_SERVERS}: {exc}")

    log.info("Producing to topic %s on %s", TOPIC, BOOTSTRAP_SERVERS)
    try:
        run(producer)
    finally:
        # Messages still in the local queue are lost unless we wait for them.
        remaining = producer.flush(15)
        log.info("Final stats: %s (still queued: %s)", stats, remaining)


if __name__ == "__main__":
    main()