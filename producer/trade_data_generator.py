import json
import time
import uuid
from datetime import datetime, timezone
import random
from confluent_kafka import Producer

def acked(err, msg):
    """Callback for delivery reports from Kafka."""
    if err is not None:
        print(f"Failed to deliver message: {str(msg)}: {str(err)}")

def generate_trade_event():
    """Generates a mock trade event."""
    symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
    trade_types = ['BUY', 'SELL']
    
    event = {
        'trade_id': str(uuid.uuid4()),
        'symbol': random.choice(symbols),
        'trade_type': random.choice(trade_types),
        'quantity': random.randint(1, 1000),
        'price': round(random.uniform(10.0, 500.0), 2),
        'event_time': datetime.now(timezone.utc).isoformat()
    }
    return event

def main():
    """Main loop to produce trade events to Kafka."""
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'trade-generator'
    }
    producer = Producer(conf)
    topic = 'trades'

    print("Starting trade data generator...")
    try:
        # Generate 10 events to verify messages are flowing
        for _ in range(10):
            event = generate_trade_event()
            # Enforcing partition hashing to guarantee event ordering
            producer.produce(
                topic,
                key=event['symbol'].encode('utf-8'),
                value=json.dumps(event).encode('utf-8'),
                callback=acked
            )
            producer.poll(0)
            print(f"Produced event: {event}")
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        print("Flushing records...")
        producer.flush()

if __name__ == '__main__':
    main()
