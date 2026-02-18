# NL2SQL Agent

Specialized Natural Language to SQL (NL2SQL) agent for Mako Group's trading desk.
Answers questions about trading data by querying BigQuery.

## Setup

1.  **Clone the repository:**
    ```bash
    git clone ...
    cd nl2sql-agent
    ```

2.  **Configure Environment:**
    Copy `.env.example` to `.env` and fill in the required values.
    ```bash
    cp nl2sql_agent/.env.example nl2sql_agent/.env
    ```

3.  **Build Docker Image:**
    ```bash
    docker compose build
    ```

## Running Tests

Run the test suite inside the Docker container:

```bash
docker compose run --rm agent pytest tests/ -v
```

## Running the Agent

Start the agent in interactive terminal mode:

```bash
docker compose run --rm agent
```

## Project Structure

- `nl2sql_agent/`: Main Python package.
    - `agent.py`: ADK agent definitions (`root_agent`, `nl2sql_agent`).
    - `config.py`: Configuration loading.
    - `protocols.py`: Interfaces for external dependencies.
    - `clients.py`: Concrete implementations of protocols.
- `setup/`: SQL scripts and schema extraction tools.
- `schemas/`: Extracted JSON schemas (not in Git).
- `catalog/`: YAML metadata catalog (Track 02).
- `tests/`: Unit tests.