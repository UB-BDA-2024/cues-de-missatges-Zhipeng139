import json
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from shared.subscriber import Subscriber


if __name__ == "__main__":
    subscriber = Subscriber()
    subscriber.consume()
    subscriber.close()