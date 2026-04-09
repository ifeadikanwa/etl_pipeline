# Product API ETL Pipeline

A simple data engineering project that extracts product data from a public REST API, transforms the raw JSON into a structured table, and loads it into a SQLite database for SQL analysis.

## Project Overview

This project demonstrates a simple ETL pipeline:

- **Extract** product data from an external API
- **Transform** the raw JSON into structured tabular data
- **Load** the cleaned data into a SQLite database
- **Query** the database using SQL for analysis

This project simulates a real-world data ingestion workflow by pulling data from an external API instead of using static files.

The project uses the DummyJSON products API as the external data source.

## Tech Stack

- Python
- Requests
- Pandas
- SQLite

## How It Works

### Extract
The pipeline sends a GET request to the API and retrieves product data in JSON format.

### Transform
The pipeline:
- converts JSON into a pandas DataFrame
- selects relevant columns (id, title, price, category, etc.)
- creates a new `discounted_price` column using price and discount percentage

### Load
The cleaned data is stored in a SQLite database (`products.db`).

### Query
A separate script runs SQL queries to analyze the data.

### Automation

The pipeline supports automated execution using a loop-based scheduler.

Run:
```bash
python run_pipeline.py
```

## Installation

```bash
git clone https://github.com/ifeadikanwa/etl_pipeline.git
cd etl-pipeline
pip install -r requirements.txt
```

## Running the Project

Run the ETL pipeline:
```bash
python etl.py
```

Run the analysis query:
```bash
python query.py
```

## Example Analysis
Example SQL query:
```sql
SELECT category, AVG(price) AS avg_price
FROM products
GROUP BY category
ORDER BY avg_price DESC;
```

This allows analysis such as:
- average price per category
- comparison of product categories
- impact of pricing differences across categories


## Future Improvements

- add API pagination to ingest more records
✓ add data validation checks before loading
✓ log pipeline steps and errors
✓ schedule the pipeline to run automatically

## Author
Ifeadikanwa Eze