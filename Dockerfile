FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency spec first (Docker layer caching)
COPY pyproject.toml .

# Install dependencies (non-editable, no source needed)
RUN pip install --no-cache-dir .

# Copy application code
COPY . .

# Re-install with source available (picks up the package properly)
RUN pip install --no-cache-dir .

# Create non-root user
RUN useradd -m agent
USER agent

# Default command: run the agent web UI
CMD ["adk", "web", "--host", "0.0.0.0", "--port", "8001", "."]
