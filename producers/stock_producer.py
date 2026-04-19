import json
import os
import time
from kafka import KafkaProducer
import yfinance as yf

TOPIC_NAME = 'stock-ticks'

producer=KafkaProducer(
    bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092'),
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

stocks = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'NVDA', 'META', 'NFLX']

def fetch_stock_data(stock):
    try:
        data=yf.Ticker(stock).history(period='1d', interval='1m')
        if not data.empty:
            latest=data.iloc[-1]
            return {
                'symbol': stock,
                'timestamp': int(latest.name.timestamp()),
                'open': latest['Open'],
                'high': latest['High'],
                'low': latest['Low'],
                'close': latest['Close'],
                'volume': int(latest['Volume'])
            }
    except Exception as e:
        print(f"Error fetching data for {stock}: {e}")
        return None
    return {
        'symbol': stock,
        'timestamp': int(time.time()),
        "price": round(100 + (hash(stock) % 100), 2),
        "volume": 1000000 + (hash(stock) % 500000),
    }

print("Starting stock data producer...")
while True:
    for stock in stocks:
        stock_data = fetch_stock_data(stock)
        if stock_data:
            producer.send(TOPIC_NAME, value=stock_data)
            print(f"Sent data for {stock}: {stock_data}")
    time.sleep(10)