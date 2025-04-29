// Configuration
const API_ENDPOINT = '/api/report';

// DOM Elements
const filterContainer = document.getElementById('filterContainer');
const tableHeader = document.getElementById('tableHeader');
const reportTableBody = document.getElementById('reportTableBody');

// State
let currentData = [];
let filters = {};

// Fields to exclude from filters
const EXCLUDED_FILTERS = [
    'id',
    'hospital_name',
    'hospital_address',
    'standard_charge_gross',
    'standard_charge_max',
    'standard_charge_min',
    'standard_charge_negotiated_dollar'
];

// Initialize the page
document.addEventListener('DOMContentLoaded', async () => {
    await fetchData();
    setupDynamicFilters();
    setupEventListeners();
});

// Fetch data from the backend
async function fetchData() {
    try {
        const response = await fetch(API_ENDPOINT);
        if (!response.ok) {
            throw new Error('Failed to fetch data');
        }
        currentData = await response.json();
        console.log(`Fetched ${currentData.length} rows`);
        setupDynamicTable();
        updateFilters();
        updateTable();
    } catch (error) {
        console.error('Error fetching data:', error);
        alert('Failed to load data. Please try again later.');
    }
}

// Setup dynamic filters based on data columns
function setupDynamicFilters() {
    if (currentData.length === 0) return;

    const columns = Object.keys(currentData[0])
        .filter(column => !EXCLUDED_FILTERS.includes(column));
    
    filterContainer.innerHTML = ''; // Clear existing filters

    columns.forEach(column => {
        const filterCol = document.createElement('div');
        filterCol.className = 'col-md-4 mb-3';
        
        const label = document.createElement('label');
        label.className = 'form-label';
        label.textContent = formatColumnName(column);
        label.htmlFor = `${column}Filter`;

        const select = document.createElement('select');
        select.className = 'form-select';
        select.id = `${column}Filter`;
        select.innerHTML = '<option value="">All</option>';

        filterCol.appendChild(label);
        filterCol.appendChild(select);
        filterContainer.appendChild(filterCol);

        // Initialize filter state
        filters[column] = '';
    });
}

// Setup event listeners for all filters
function setupEventListeners() {
    Object.keys(filters).forEach(column => {
        const filter = document.getElementById(`${column}Filter`);
        if (filter) {
            filter.addEventListener('change', (e) => {
                filters[column] = e.target.value;
                updateFilters();
                updateTable();
            });
        }
    });
}

// Setup dynamic table headers
function setupDynamicTable() {
    if (currentData.length === 0) return;

    const headerRow = document.createElement('tr');
    Object.keys(currentData[0]).forEach(column => {
        const th = document.createElement('th');
        th.textContent = formatColumnName(column);
        headerRow.appendChild(th);
    });
    tableHeader.innerHTML = '';
    tableHeader.appendChild(headerRow);
}

// Update filter options based on current selections
function updateFilters() {
    Object.keys(filters).forEach(column => {
        const filter = document.getElementById(`${column}Filter`);
        if (!filter) return;

        // Get unique values for this column based on other filters
        const uniqueValues = [...new Set(currentData
            .filter(item => {
                return Object.entries(filters).every(([key, value]) => {
                    if (key === column) return true;
                    return !value || String(item[key]) === value;
                });
            })
            .map(item => item[column]))];

        // Sort values appropriately
        uniqueValues.sort((a, b) => {
            if (typeof a === 'number' && typeof b === 'number') {
                return a - b;
            }
            return String(a).localeCompare(String(b));
        });

        updateSelectOptions(filter, uniqueValues);
    });
}

// Helper function to update select options
function updateSelectOptions(selectElement, options) {
    const currentValue = selectElement.value;
    selectElement.innerHTML = '<option value="">All</option>';
    
    options.forEach(option => {
        const optionElement = document.createElement('option');
        optionElement.value = String(option);
        optionElement.textContent = formatValue(option);
        selectElement.appendChild(optionElement);
    });

    if (options.includes(currentValue)) {
        selectElement.value = currentValue;
    }
}

// Update the table with filtered data
function updateTable() {
    const filteredData = currentData.filter(item => {
        return Object.entries(filters).every(([key, value]) => {
            return !value || String(item[key]) === value;
        });
    });

    reportTableBody.innerHTML = '';
    filteredData.forEach(item => {
        const row = document.createElement('tr');
        Object.entries(item).forEach(([key, value]) => {
            const td = document.createElement('td');
            td.textContent = formatValue(value);
            // Add class for numeric columns
            if (typeof value === 'number' || (value && !isNaN(value))) {
                td.className = 'text-end'; // Right-align numbers
            }
            row.appendChild(td);
        });
        reportTableBody.appendChild(row);
    });

    // Update results count
    console.log(`Displaying ${filteredData.length} rows`);
}

// Helper function to format column names
function formatColumnName(column) {
    return column
        .split('_')
        .map(word => word.charAt(0).toUpperCase() + word.slice(1))
        .join(' ');
}

// Helper function to format values
function formatValue(value) {
    if (value === null || value === undefined) return '';
    
    // Handle numeric values
    if (typeof value === 'number' || (typeof value === 'string' && !isNaN(value))) {
        const num = parseFloat(value);
        // Format as currency if it's a charge/price column
        if (String(value).includes('.') && 
            (String(value).includes('charge') || String(value).includes('price'))) {
            return new Intl.NumberFormat('en-US', { 
                style: 'currency', 
                currency: 'USD' 
            }).format(num);
        }
        // Format other numbers with appropriate decimals
        return new Intl.NumberFormat('en-US', {
            minimumFractionDigits: Number.isInteger(num) ? 0 : 2,
            maximumFractionDigits: 2
        }).format(num);
    }
    
    return String(value);
} 