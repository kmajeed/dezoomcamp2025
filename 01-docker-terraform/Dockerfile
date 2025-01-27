# Use the official Python 3.12.8 image as a base
FROM python:3.12.8

# Set the working directory inside the container
WORKDIR /app

# Install system dependencies if needed (example: for building certain Python packages)
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     build-essential \
#     && rm -rf /var/lib/apt/lists/*

# Copy your application code if you have any
# COPY . /app

# Set the entrypoint to bash for interactive mode
ENTRYPOINT ["/bin/bash"]

# Add a command to check pip version inside the container
# RUN pip --version

# or for more detailed output
# RUN pip show pip