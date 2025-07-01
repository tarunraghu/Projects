# 🏥 MediPriceInsight Application

Healthcare Price Transparency & Data Management Platform

## 📋 Prerequisites

- Python 3.7 or higher installed
- Required Python packages (install using: `pip install -r requirements.txt`)
- All project files in the correct directory structure

## 🚀 Quick Start

### Step 1: Open Command Prompt

1. Press **Windows + R**
2. Type `cmd` and press **Enter**
3. Navigate to your project directory:
   ```bash
   cd "C:\Users\Docker\Documents\GitHub\Projects\MediPriceInsight Project"
   ```

### Step 2: Start All Applications

Run the following command to start all applications:
```bash
python start_all_apps.py
```

This will start:
- **Main Application (Homepage)**: http://localhost:5000
- **Type 1 CSV Direct Ingestion**: http://localhost:5001
- **Type 2 CSV Pivot Ingestion**: http://localhost:5002
- **Type 3 JSON Direct Ingestion**: http://localhost:5003
- **Data Dump Application**: http://localhost:5004
- **Price Transparency Report**: http://localhost:5005

### Step 3: Access the Applications

1. Open your web browser
2. Visit [http://localhost:5000](http://localhost:5000) for the main homepage
3. Use the homepage to navigate to different applications
4. Or directly access individual applications at their respective ports

## 🛠️ Application Management

While the applications are running, you can use these commands in the terminal:

| Command | Description |
|---------|-------------|
| `status` | Show current status of all applications |
| `restart` | Restart all applications |
| `stop` | Stop all applications |
| `exit` | Stop all applications and exit |
| `help` | Show available commands |

## 🛑 Stopping Applications

To stop all applications:
1. In the terminal where you ran `start_all_apps.py`, type `stop`
2. Or press **Ctrl+C** to stop all applications
3. Or type `exit` to stop and close the terminal

## 📁 Project Structure

```
MediPriceInsight Project/
├── main_app.py                 # Main homepage application (Port 5000)
├── start_all_apps.py          # Application launcher
├── db_config.py               # Database configuration
├── db_script.sql              # Database creation script
├── README.md                  # This file
├── MediPriceInsight Project - Type 1/    # CSV Direct Ingestion (Port 5001)
├── MediPriceInsight Project - Type 2/    # CSV Pivot Ingestion (Port 5002)
├── MediPriceInsight Project - Type 3/    # JSON Direct Ingestion (Port 5003)
├── Request Data Dump/         # Data Dump Application (Port 5004)
├── Price Transparency Report/ # Price Transparency Report (Port 5005)
└── templates/
    └── index.html             # Main homepage template
```

## 🔧 Features

- **Dynamic Path Handling**: All applications use dynamic paths and work from any directory
- **Independent Applications**: Each application runs on its own port
- **Beautiful UI**: Modern, responsive design with Bootstrap and custom styling
- **Database Integration**: PostgreSQL database support with comprehensive schema
- **File Processing**: Support for CSV and JSON file ingestion
- **Data Export**: Built-in data dump and export functionality

## 📊 Database Setup

To set up the database:

1. **Create Database**: Run the database script:
   ```bash
   psql -U postgres -f db_script.sql
   ```

2. **Database Schema**: The complete PostgreSQL table schema is present in `db_script.sql` which includes:
   - Database creation (`CREATE DATABASE IF NOT EXISTS healthcarepoc`)
   - All table definitions (hospital_info, charge_data, pivot_data, json_data, audit_log, etc.)
   - Indexes for performance optimization
   - Stored procedures and functions
   - Sample data insertion (commented out)

3. **Database Configuration**: Update `db_config.py` with your database credentials:
   ```python
   DB_CONFIG = {
       'host': 'localhost',
       'database': 'healthcarepoc',
       'user': 'postgres',
       'password': 'Consis10C!',
       'port': '5432'
   }
   ```

## 🐛 Troubleshooting

### Common Issues

1. **Directory Name Invalid Error**:
   - Make sure you're in the correct project directory
   - All paths are now dynamic and should work from any location

2. **Port Already in Use**:
   - Stop any existing applications first
   - Use `netstat -ano | findstr :5000` to check if ports are in use

3. **Application Not Loading**:
   - Check if all applications started successfully
   - Verify the correct ports are being used
   - Check browser console for any JavaScript errors

### Getting Help

- Check the README.md files in each application directory for specific instructions
- Review the console output for error messages
- Ensure all required dependencies are installed

## 💾 Storage Management

### Free Up Storage Space

If you want to increase available storage, you can delete files from the uploads folders:

```bash
# Clean Type 1 uploads folder
rm -rf "MediPriceInsight Project - Type 1/uploads/*"

# Clean Type 2 uploads folder  
rm -rf "MediPriceInsight Project - Type 2/uploads/*"

# Clean Type 3 uploads folder
rm -rf "MediPriceInsight Project - Type 3/uploads/*"

# Clean Data Dump downloads folder
rm -rf "Request Data Dump/downloads/*"
```

**⚠️ Warning**: This will permanently delete all uploaded files. Make sure you have processed and saved any important data before cleaning these folders.

### Manual Cleanup

You can also manually delete files from these directories:
- `MediPriceInsight Project - Type 1/uploads/`
- `MediPriceInsight Project - Type 2/uploads/`
- `MediPriceInsight Project - Type 3/uploads/`
- `Request Data Dump/downloads/`

## 🐛 Debugging Individual Applications

### Debug Ingestion Applications

If you want to debug each ingestion application individually, you need to run the specific Flask application and check the command prompt for errors:

#### Type 1 CSV Direct Ingestion
```bash
cd "MediPriceInsight Project - Type 1"
python app.py
```
- Access at: http://localhost:5001
- Check command prompt for any error messages or logs

#### Type 2 CSV Pivot Ingestion
```bash
cd "MediPriceInsight Project - Type 2"
python app.py
```
- Access at: http://localhost:5002
- Check command prompt for any error messages or logs

#### Type 3 JSON Direct Ingestion
```bash
cd "MediPriceInsight Project - Type 3"
python app.py
```
- Access at: http://localhost:5003
- Check command prompt for any error messages or logs

#### Data Dump Application
```bash
cd "Request Data Dump"
python app.py
```
- Access at: http://localhost:5004
- Check command prompt for any error messages or logs

#### Price Transparency Report
```bash
cd "Price Transparency Report"
python app.py
```
- Access at: http://localhost:5005
- Check command prompt for any error messages or logs

### Debugging Tips

1. **Check Command Prompt**: All error messages, logs, and debugging information will appear in the command prompt where you started the application
2. **Stop Other Applications**: Make sure to stop other applications first to avoid port conflicts
3. **Review Logs**: Look for any error messages, warnings, or informational logs in the console output
4. **Database Connection**: Verify database connection settings in `db_config.py`
5. **File Permissions**: Ensure the application has read/write permissions to the uploads folder

## 📝 Notes

- All applications use dynamic paths and should work from any directory
- The main homepage (port 5000) serves the existing `index.html` template
- Each application runs independently on its own port
- Make sure all required dependencies are installed before running

## 🤝 Contributing

For additional help, check the README.md files in each application directory.

---

**MediPriceInsight** - Healthcare Price Transparency & Data Management Platform 