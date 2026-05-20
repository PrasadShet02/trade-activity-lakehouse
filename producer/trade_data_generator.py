import json
import random
import time
import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer

TOPIC_NAME = "trades"
KAFKA_SERVER = "localhost:9092"

symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'GS', 'JPM']
trade_types = ['BUY', 'SELL']
exchanges = ['NASDAQ', 'NYSE', 'LSE']

# In-memory pool of active trades to drive state transitions
active_trades = {}

def acked(err, msg):
    if err is not None:
        print(f"Delivery failed: {str(err)}")

def generate_trade_event():
    """
    Simulates a realistic trade lifecycle: PENDING -> EXECUTED -> SETTLED.
    30% of the time, progress an existing active trade's state.
    """
    if active_trades and random.random() < 0.3:
        trade_id = random.choice(list(active_trades.keys()))
        trade = active_trades[trade_id].copy()

        # Advance the trade through its lifecycle
        if trade['status'] == 'PENDING':
            trade['status'] = 'EXECUTED'
            trade['price'] = round(trade['price'] * random.uniform(0.98, 1.02), 2)
        elif trade['status'] == 'EXECUTED':
            trade['status'] = 'SETTLED'

        trade['event_time'] = datetime.now(timezone.utc).isoformat()
        active_trades[trade_id] = trade

        if trade['status'] == 'SETTLED':
            del active_trades[trade_id]

        return trade
    else:
        trade_id = str(uuid.uuid4())
        new_trade = {
            "trade_id": trade_id,
            "symbol": random.choice(symbols),
            "price": round(random.uniform(100.0, 1500.0), 2),
            "quantity": random.randint(1, 500),
            "trade_type": random.choice(trade_types),
            "status": "PENDING",
            "exchange": random.choice(exchanges),
            "event_time": datetime.now(timezone.utc).isoformat()
        }
        active_trades[trade_id] = new_trade
        return new_trade


def main():
    conf = {
        'bootstrap.servers': KAFKA_SERVER,
        'client.id': 'trade-generator'
    }
    producer = Producer(conf)

    print(f"Starting stateful trade generator. Publishing to topic: '{TOPIC_NAME}'")

    try:
        while True:
            event = generate_trade_event()

            # Enforcing partition hashing to guarantee event ordering per symbol
            producer.produce(
                TOPIC_NAME,
                key=event['symbol'].encode('utf-8'),
                value=json.dumps(event).encode('utf-8'),
                callback=acked
            )
            producer.poll(0)
            print(f"Event -> {event['trade_id'][:8]}... | {event['symbol']} | {event['status']} | ${event['price']}")
            time.sleep(1)

    except KeyboardInterrupt:
        print("Generator stopped.")
    finally:
        producer.flush()


if __name__ == '__main__':
    main()
