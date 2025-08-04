# file: test_connection.py
import asyncio
import websockets
import logging

# Set up simple logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

async def check_binance_ws():
    """Attempts to connect to a single Binance WebSocket stream."""
    # A standard, reliable Binance stream
    uri = "wss://fstream.binance.com/ws/btcusdt@kline_1m"
    
    logging.info(f"Attempting to connect to: {uri}")
    
    try:
        # Set a timeout for the connection attempt
        async with websockets.connect(uri, open_timeout=10) as ws:
            logging.info("SUCCESS: WebSocket connection established!")
            
            # Receive one message to confirm it's working
            message = await asyncio.wait_for(ws.recv(), timeout=5)
            logging.info(f"SUCCESS: Received first message: {message[:100]}...")
            
    except websockets.exceptions.ConnectionClosed as e:
        logging.error(f"FAIL: Connection was closed by the server. Reason: {e}")
    except asyncio.TimeoutError:
        logging.error("FAIL: The connection attempt timed out. This strongly suggests a network block.")
    except Exception as e:
        logging.error(f"FAIL: An unexpected error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(check_binance_ws())
