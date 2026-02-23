import json
import os
import time
import random
import logging
import psycopg2
from confluent_kafka import Consumer, KafkaError
import redis

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("MLService")

# Config from env
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
DB_CONN_STR = os.getenv("DB_CONN_STR", "postgres://myappuser:mypassword@localhost:5432/myapp")

def predict_neobert(text):
    """
    Mock prediction logic for NeoBERT.
    Returns a random tag for demonstration.
    """
    tags = ["Programming", "Biology", "Finance", "Legal", "General"]
    # Simulate some work
    time.sleep(0.5)
    return random.choice(tags)

def update_db(conn, file_id, tag):
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE files SET tag = %s, status = 'READY', error_msg = NULL WHERE id = %s",
                (tag, file_id)
            )
        conn.commit()
        logger.info(f"Updated file {file_id} with tag {tag}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to update DB for file {file_id}: {e}")

def mark_as_error(conn, file_id, error_msg):
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE files SET status = 'ERROR', error_msg = %s WHERE id = %s",
                (error_msg, file_id)
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to mark error for file {file_id}: {e}")

def main():
    logger.info("ML Service starting...")

    # Initialize connections
    r_client = redis.from_url(REDIS_URL, decode_responses=True)
    db_conn = psycopg2.connect(DB_CONN_STR)
    
    consumer_conf = {
        'bootstrap.servers': KAFKA_BROKERS,
        'group.id': 'ml-service-group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe(['text-to-classify'])

    logger.info(f"ML Service active. Listening on {KAFKA_BROKERS}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                    break

            try:
                payload = json.loads(msg.value().decode('utf-8'))
                file_id = payload.get('file_id')
                cache_key = payload.get('cache_key')

                if not file_id or not cache_key:
                    logger.warning(f"Invalid payload: {payload}")
                    continue

                logger.info(f"Processing file {file_id} from cache {cache_key}")

                # 1. Get Text from Redis
                text = r_client.get(cache_key)
                if not text:
                    logger.error(f"Text not found in cache for key {cache_key}")
                    mark_as_error(db_conn, file_id, "Extracted text expired or not found")
                    continue

                # 2. NeoBERT Inference (Mocked)
                tag = predict_neobert(text)

                # 3. Persistence
                update_db(db_conn, file_id, tag)

                # 4. Cleanup Redis
                r_client.delete(cache_key)
                logger.info(f"Cleaned up cache for file {file_id}")

            except Exception as e:
                logger.error(f"Internal error processing message: {e}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        db_conn.close()
        logger.info("ML Service stopped")

if __name__ == "__main__":
    main()
