# Base image
FROM python:3.11.5-slim

# Working directory
WORKDIR /

# Copy project files
COPY . .

# Install Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Install curl for downloading Google Cloud SDK
RUN apt-get update && apt-get install -y curl

# Download and install Google Cloud SDK
RUN curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-467.0.0-linux-x86_64.tar.gz && \
    tar zxvf google-cloud-sdk-467.0.0-linux-x86_64.tar.gz && \
    ./google-cloud-sdk/install.sh --quiet

# Add Google Cloud SDK to the PATH
ENV PATH $PATH:/app/google-cloud-sdk/bin

# Default command
RUN chmod +x run_movie_data_pipeline.sh
CMD ["./run_movie_data_pipeline.sh"]
