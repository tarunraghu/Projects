# MediPriceInsight

A web application for ingesting and processing healthcare pricing data.

## Setup

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. Ensure PostgreSQL is running and create a database named `healthcarepoc`

3. Place the PostgreSQL JDBC driver (`postgresql-42.7.2.jar`) in the root directory

4. Start the application:
```bash
python app.py
```

## Usage

1. Open a web browser and navigate to `http://localhost:5000`

2. Select the ingestion strategy and file type

3. Enter the file path and click "Submit"

4. Review the preview data and confirm ingestion

## Project Structure

- `app.py`: Main Flask application
- `index.html`: Web interface
- `ingestion_strategies/`: Data ingestion strategy implementations
  - `base_strategy.py`: Base class for ingestion strategies
  - `type1_strategy.py`: Type 1 ingestion strategy implementation
- `postgresql-42.7.2.jar`: PostgreSQL JDBC driver

## Database Schema

### addresses
- Contains hospital address information
- First 5 columns from input file

### hospital_charges
- Contains hospital charge information
- Columns: hospital_name, code, charge

### hospital_log
- Logs ingestion attempts
- Columns: id, user_name, file_type, file_path, ingestion_strategy, ingestion_timestamp, ingestion_status, error_message 