FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies first (Docker layer caching)
COPY pyproject.toml .
RUN pip install --no-cache-dir -e .

# Copy application code
COPY . .

# Install the package in editable mode (now with all source files)
RUN pip install --no-cache-dir -e ".[dev]"

# Default command: run the agent in terminal mode
# adk run expects to be in the PARENT of the agent package directory
CMD ["adk", "run", "nl2sql_agent"]