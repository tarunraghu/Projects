# Price Transparency Report

A web application that displays a tabular report of healthcare prices with cascading filters, powered by PostgreSQL.

## Features

- Interactive tabular report
- Cascading filters for Hospital, Service, and Insurance
- Real-time data updates
- Responsive design
- PostgreSQL integration

## Setup Instructions

1. Ensure you have PostgreSQL installed and running
2. Create a database and import the required schema
3. Set up your backend API to serve data at `/api/report`
4. Install dependencies:
   ```bash
   npm install
   ```
5. Start the development server:
   ```bash
   npm start
   ```

## Database Schema

The application expects the following data structure:

```sql
CREATE TABLE price_reports (
    id SERIAL PRIMARY KEY,
    hospital VARCHAR(255) NOT NULL,
    service VARCHAR(255) NOT NULL,
    insurance VARCHAR(255) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    date DATE NOT NULL
);
```

## API Endpoint

The frontend expects a JSON response from `/api/report` with the following structure:

```json
[
    {
        "hospital": "Hospital Name",
        "service": "Service Name",
        "insurance": "Insurance Plan",
        "price": 1000.00,
        "date": "2024-03-20"
    }
]
```

## Development

To modify the frontend:
- Edit `index.html` for structure
- Edit `styles.css` for styling
- Edit `app.js` for functionality

## License

MIT 