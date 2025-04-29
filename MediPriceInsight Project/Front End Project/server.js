const express = require('express');
const path = require('path');
const { Pool } = require('pg');
const fs = require('fs');

// Load configuration
const config = JSON.parse(fs.readFileSync('./config.json', 'utf8'));

const app = express();
const port = config.server.port || 3000;

// PostgreSQL connection configuration
const pool = new Pool(config.db);

app.use(express.json());
app.use(express.static('public'));

// Serve initial filter options
app.get('/api/filters', async (req, res) => {
    try {
        const filterQueries = {
            state: 'SELECT DISTINCT state FROM healthcare_data ORDER BY state',
            city: 'SELECT DISTINCT city FROM healthcare_data ORDER BY city',
            hospital: 'SELECT DISTINCT hospital FROM healthcare_data ORDER BY hospital',
            procedure: 'SELECT DISTINCT procedure FROM healthcare_data ORDER BY procedure',
            insurance: 'SELECT DISTINCT insurance FROM healthcare_data ORDER BY insurance'
        };

        const results = {};
        for (const [key, query] of Object.entries(filterQueries)) {
            const { rows } = await pool.query(query);
            results[key] = rows.map(row => row[key]);
        }

        res.json(results);
    } catch (error) {
        console.error('Error fetching filters:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// Handle dependent filters
app.post('/api/dependent-filters', async (req, res) => {
    try {
        const filters = req.body;
        let baseQuery = 'SELECT DISTINCT ';
        let whereConditions = [];
        let values = [];
        let paramCount = 1;

        // Build the WHERE clause based on selected filters
        Object.entries(filters).forEach(([key, values]) => {
            if (values && values.length > 0) {
                whereConditions.push(`${key} = ANY($${paramCount})`);
                values.push(values);
                paramCount++;
            }
        });

        // Get updated options for each filter type
        const results = {};
        const filterTypes = ['state', 'city', 'hospital', 'procedure', 'insurance'];

        for (const filterType of filterTypes) {
            let query = `
                SELECT DISTINCT ${filterType}
                FROM healthcare_data
                ${whereConditions.length > 0 ? 'WHERE ' + whereConditions.join(' AND ') : ''}
                ORDER BY ${filterType}
            `;
            
            const { rows } = await pool.query(query, values);
            results[filterType] = rows.map(row => row[filterType]);
        }

        res.json(results);
    } catch (error) {
        console.error('Error fetching dependent filters:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// Handle filtered data
app.post('/api/filtered-data', async (req, res) => {
    try {
        const filters = req.body;
        let whereConditions = [];
        let values = [];
        let paramCount = 1;

        // Build the WHERE clause based on selected filters
        Object.entries(filters).forEach(([key, filterValues]) => {
            if (filterValues && filterValues.length > 0) {
                whereConditions.push(`${key} = ANY($${paramCount})`);
                values.push(filterValues);
                paramCount++;
            }
        });

        // Construct and execute the query
        let query = `
            SELECT *
            FROM healthcare_data
            ${whereConditions.length > 0 ? 'WHERE ' + whereConditions.join(' AND ') : ''}
            ORDER BY state, city, hospital, procedure
            LIMIT ${config.api.max_results}
        `;

        const { rows } = await pool.query(query, values);
        res.json(rows);
    } catch (error) {
        console.error('Error fetching filtered data:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.listen(port, () => {
    console.log(`Server is running on port ${port}`);
}); 