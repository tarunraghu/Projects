# MediPriceInsight - Healthcare Data Processing Application (Type 1)

## Overview

MediPriceInsight is a Flask-based web application designed to process and manage healthcare pricing data from hospital standard charge files. This application specifically handles **Type 1 ingestion** workflow for hospital charge data processing.

## Features

### Core Functionality
- **File Upload & Processing**: Upload CSV files containing hospital standard charge data
- **Data Validation**: Automatic validation of file structure and data integrity
- **Type 1 Ingestion**: Specialized processing workflow for Type 1 data format
- **Address Management**: Separate processing of hospital address information
- **Data Preview**: Preview data before ingestion to ensure accuracy
- **Background Processing**: Asynchronous data processing with progress tracking
- **Data Archiving**: Automatic archiving of existing records before new data ingestion
- **Data Export**: Generate comprehensive data dumps for analysis

### Technical Features
- **Database Integration**: PostgreSQL backend with connection pooling
- **Big Data Processing**: PySpark integration for large dataset processing
- **Session Management**: Secure session handling for multi-step workflows
- **Error Handling**: Comprehensive error logging and user feedback
- **Progress Tracking**: Real-time progress updates for long-running operations

## Architecture

### Technology Stack
- **Backend**: Flask (Python web framework)
- **Database**: PostgreSQL with psycopg2
- **Data Processing**: Apache Spark (PySpark)
- **Frontend**: HTML, CSS, JavaScript
- **Data Handling**: Pandas, SQLAlchemy

### Key Components

#### 1. Database Schema
- `hospital_address`: Hospital location and contact information
- `hospital_charges`: Main charge data with pricing information
- `hospital_charges_archive`: Archived records for audit trail
- `hospital_log`: Processing logs and audit information

#### 2. Core Classes
- **SparkManager**: Singleton pattern for Spark session management
- **BackgroundTask**: Task management for asynchronous processing

#### 3. Processing Workflow
- **Type 1 Ingestion**: Standard processing workflow for Type 1 data format

## Installation & Setup

### Prerequisites
- Python 3.7+
- PostgreSQL database (with existing tables)
- Apache Spark (for data processing)
- Java 8+ (required for Spark)

### Dependencies
```bash
pip install flask flask-cors psycopg2-binary pyspark pandas sqlalchemy
```

### Database Setup
1. Ensure PostgreSQL database is running
2. Verify required tables exist in your backend database:
   - `hospital_address`
   - `hospital_charges`
   - `hospital_charges_archive`
   - `hospital_log`
3. Configure database connection in environment variables

### Environment Variables
```bash
DB_NAME=healthcarepoc
DB_USER=postgres
DB_PASSWORD=Consis10C!
DB_HOST=localhost
DB_PORT=5432
```

## Usage

### Starting the Application
```bash
python app.py
```
The application will start on `http://localhost:5001`

### Workflow

#### 1. File Upload
- Navigate to the home page
- Select a CSV file containing hospital charge data (Type 1 format)
- Provide user information

#### 2. Address Review
- Review extracted hospital address information
- Confirm or modify address details
- Add region and city information

#### 3. Data Preview
- Preview processed charge data
- Verify column mappings and data transformations
- Check for any data quality issues

#### 4. Data Ingestion
- Initiate background processing
- Monitor progress through real-time updates
- Review processing results and statistics

#### 5. Data Management
- Generate data dumps for analysis
- Archive inactive records
- View processing logs

## Data Processing

### Type 1 Ingestion Workflow
1. **File Upload**: Upload CSV file containing hospital standard charge data
2. **File Splitting**: Automatically split into address and charges files
3. **Address Review**: Review and confirm hospital address information
4. **Data Preview**: Preview processed charges data before ingestion
5. **Background Processing**: Process charges data in background with progress tracking
6. **Data Storage**: Store processed data in PostgreSQL database

### Supported Data Types
- **CPT Codes**: Current Procedural Terminology codes (Primary supported code type)
- **Standard Charges**: Gross, negotiated, minimum, and maximum charges
- **Payer Information**: Insurance provider and plan details

### Data Transformations
- Automatic column mapping and validation
- Data type conversion and cleaning
- Duplicate detection and removal
- Null value handling

## Configuration

### Spark Configuration
```python
SparkSession.builder \
    .appName("Healthcare Data Processing") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "10")
```

### Database Configuration
- Connection pooling (1-20 connections)
- Automatic connection management
- Transaction handling with rollback support

## Error Handling

### Comprehensive Error Management
- File validation errors
- Database connection issues
- Data processing failures
- User input validation

### Logging
- Structured logging with different levels
- Error tracking and debugging information
- Processing audit trails

## Security Features

### Session Management
- Secure session handling
- User authentication tracking
- Session timeout management

### Data Protection
- Input validation and sanitization
- SQL injection prevention
- File upload security

## Performance Optimization

### Database Optimization
- Connection pooling
- Prepared statements
- Index optimization
- Batch processing

### Data Processing
- Spark partitioning
- Memory management
- Parallel processing
- Chunked data handling


## Monitoring & Maintenance

### Health Checks
- Database connection monitoring
- Spark session management
- Application status tracking

### Cleanup Operations
- Temporary file cleanup
- Spark temp directory management
- Database connection cleanup

## Troubleshooting

### Common Issues
1. **Database Connection**: Check PostgreSQL service and credentials
2. **Spark Issues**: Verify Java installation and Spark configuration
3. **File Processing**: Ensure CSV format compliance (Type 1 format)
4. **Memory Issues**: Adjust Spark memory settings

### Debug Mode
Enable debug mode for detailed error information:
```python
app.run(host='0.0.0.0', port=5001, debug=True)
```

## Contributing

### Development Setup
1. Clone the repository
2. Install dependencies
3. Configure database
4. Run in development mode

### Code Standards
- Follow PEP 8 Python style guidelines
- Add comprehensive error handling
- Include proper documentation
- Write unit tests for new features

