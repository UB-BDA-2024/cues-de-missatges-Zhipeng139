#!/bin/sh

# Start the first consumer
python /app/consumer/main.py /app/consumer/config1.json 


# Wait for all background processes to complete
wait