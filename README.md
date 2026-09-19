# AdventureWorks Data Engineering Project

End-to-end Data Engineering pipeline built using PySpark and Delta Lake.

## Technology Stack

- Python 3.12
- PySpark 4.2
- Delta Lake 4.4
- Java 17
- Pytest
- GitHub Actions

## Architecture

Landing -> Bronze -> Silver -> Gold -> Data Quality -> Monitoring

## Pipeline

The pipeline contains 23 stages and implements incremental processing, idempotency, retry handling, monitoring, data-quality validation, and performance optimization.

## Testing

The project contains 13 automated pytest tests covering both positive and negative data-quality scenarios.

## CI/CD

GitHub Actions installs Python 3.12, Java 17, project dependencies, validates Python syntax, and runs the automated tests.

## Running the Pipeline

```bash
PYTHONPATH=src python src/pipeline.py
```

## Running Tests

```bash
PYTHONPATH=. pytest -v
```
