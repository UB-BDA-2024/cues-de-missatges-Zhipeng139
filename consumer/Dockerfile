# Use an appropriate base image
FROM python:3.11.1-alpine3.17
# Set the working directory inside the Docker image
WORKDIR /app

# Copy the shared directory from the host to the image
COPY ./shared/ shared/

# Copy the consumer directory from the host to the image
COPY ./consumer consumer/

# You can add more instructions here, like installing dependencies, etc.
# For example, to install dependencies from requirements.txt:
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Copy the start script
COPY ./consumer/start.sh ./start.sh
RUN chmod +x ./start.sh

# Set PYTHONPATH to include the shared directory
ENV PYTHONPATH="/app/shared:/app/consumer"

# Command to run when the container starts
CMD ["./start.sh"]
