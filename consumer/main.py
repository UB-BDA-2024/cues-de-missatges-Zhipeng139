import json
import os
import sys
import logging

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from shared.subscriber import Subscriber

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    print("Starting subscriber...")
    if len(sys.argv) != 2:
        print("Usage: python main.py <config_file>")
        sys.exit(1)

    config_file = sys.argv[1]
    
    try:
        with open(config_file, 'r') as file:
            config = json.load(file)
    except Exception as e:
        logging.error(f"Failed to load config file {config_file}: {e}")
        sys.exit(1)

    try:
        subscriber = Subscriber(config)
        subscriber.consume()
    except Exception as e:
        logging.error(f"Failed to start subscriber: {e}")
        sys.exit(1)
    finally:
        subscriber.close()
