// Configuration
const API_ENDPOINT = '/api/report';
const FILTERS_ENDPOINT = '/api/filters';

// DOM Elements
const filterContainer = document.getElementById('filterContainer');
const tableHeader = document.getElementById('tableHeader');
const reportTableBody = document.getElementById('reportTableBody');
const paginationContainer = document.createElement('div');
paginationContainer.className = 'pagination-container mt-3 d-flex justify-content-between align-items-center';
document.querySelector('.table-responsive').after(paginationContainer);

// Add loading indicator
const loadingIndicator = document.createElement('div');
loadingIndicator.className = 'loading-indicator';
loadingIndicator.innerHTML = `
    <div class="position-fixed top-50 start-50 translate-middle">
        <div class="spinner-border text-primary" role="status">
            <span class="visually-hidden">Loading...</span>
        </div>
    </div>
`;
document.body.appendChild(loadingIndicator);

// Constants
const EXCLUDED_FILTERS = [
    'id',
    'hospital_name',
    'hospital_address',
    'standard_charge_gross',
    'standard_charge_max',
    'standard_charge_min',
    'standard_charge_negotiated_dollar'
];

const FILTER_ORDER = ['code', 'region', 'city', 'payer_name', 'plan_name'];
const MANDATORY_FILTERS = ['code'];
const DEBOUNCE_DELAY = 300;

// State management
const state = {
    currentData: [],
    filters: {},
    filterOptions: {},
    currentPage: 1,
    perPage: 100,
    totalPages: 1,
    isLoading: false,
    lastFetchTime: 0,
    sortColumn: null,
    sortDirection: 'asc'
};

// Initialize the page
document.addEventListener('DOMContentLoaded', async () => {
    try {
        showLoading();
        await setupFilters();
        await fetchData();
        setupEventListeners();
        updateTable();
    } catch (error) {
        console.error('Error during initialization:', error);
        showError('Failed to initialize the application. Please try again later.');
    } finally {
        hideLoading();
    }
});

// Loading state management
function showLoading() {
    state.isLoading = true;
    loadingIndicator.style.display = 'block';
}

function hideLoading() {
    state.isLoading = false;
    loadingIndicator.style.display = 'none';
}

// Error handling
function showError(message) {
    alert(message);
}

// Debounce function
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

// Fetch filter values
async function setupFilters() {
    try {
        console.log('Fetching filter values...');
        const response = await fetch(FILTERS_ENDPOINT);
        if (!response.ok) throw new Error('Failed to fetch filter values');
        
        const filterValues = await response.json();
        console.log('Filter values:', filterValues);
        
        setupDynamicFilters(Object.keys(filterValues));
        populateFilters(filterValues);
    } catch (error) {
        console.error('Error fetching filter values:', error);
        throw error;
    }
}

// Fetch data from the backend
async function fetchData(page = 1) {
    const now = Date.now();
    if (now - state.lastFetchTime < DEBOUNCE_DELAY) {
        console.log('Throttling API call');
        return;
    }
    
    try {
        showLoading();
        console.log('Fetching data from API...');
        
        const queryParams = new URLSearchParams({
            page: page,
            per_page: state.perPage,
            ...Object.fromEntries(Object.entries(state.filters).filter(([_, value]) => value !== ''))
        });

        const response = await fetch(`${API_ENDPOINT}?${queryParams}`);
        if (!response.ok) throw new Error('Failed to fetch data');
        
        const result = await response.json();
        state.currentData = result.data;
        state.totalPages = result.total_pages;
        state.currentPage = page;
        
        console.log(`Fetched ${state.currentData.length} rows (page ${page} of ${state.totalPages})`);
        state.lastFetchTime = now;
        
        updatePagination();
        return result;
    } catch (error) {
        console.error('Error in fetchData:', error);
        showError('Failed to load data. Please try again later.');
        throw error;
    } finally {
        hideLoading();
    }
}

// Setup dynamic filters
function setupDynamicFilters(columns) {
    console.log('Setting up dynamic filters...');
    filterContainer.innerHTML = '';

    // Create filters in specified order
    FILTER_ORDER.forEach(column => {
        if (columns.includes(column)) {
            console.log(`Creating filter for ${column}...`);
            createFilterElement(column, MANDATORY_FILTERS.includes(column));
        }
    });
}

// Create a filter element
function createFilterElement(column, isMandatory) {
    const filterCol = document.createElement('div');
    filterCol.className = 'col-md-4 mb-3';
    
    const label = document.createElement('label');
    label.className = 'form-label';
    label.textContent = column === 'code' ? 'Code or Description *' : formatColumnName(column);
    label.htmlFor = `${column}Filter`;

    if (column === 'code') {
        // Create searchable dropdown for code
        const select = document.createElement('select');
        select.className = 'form-select';
        select.id = `${column}Filter`;
        
        filterCol.appendChild(label);
        filterCol.appendChild(select);

        $(select).select2({
            theme: 'bootstrap-5',
            width: '100%',
            placeholder: 'Search by code or description...',
            allowClear: true
        });
    } else {
        // Create multi-select container for other filters
        const filterContainer = document.createElement('div');
        filterContainer.className = 'multi-select-container';
        filterContainer.id = `${column}Container`;

        // Add select all option
        const selectAllDiv = document.createElement('div');
        selectAllDiv.className = 'form-check mb-2';
        
        const selectAllInput = document.createElement('input');
        selectAllInput.type = 'checkbox';
        selectAllInput.className = 'form-check-input select-all';
        selectAllInput.id = `${column}SelectAll`;
        
        const selectAllLabel = document.createElement('label');
        selectAllLabel.className = 'form-check-label';
        selectAllLabel.htmlFor = `${column}SelectAll`;
        selectAllLabel.textContent = 'Select All';
        
        selectAllDiv.appendChild(selectAllInput);
        selectAllDiv.appendChild(selectAllLabel);
        
        // Add search input for options
        const searchInput = document.createElement('input');
        searchInput.type = 'text';
        searchInput.className = 'form-control mb-2';
        searchInput.placeholder = `Search ${formatColumnName(column)}...`;
        
        // Add options container
        const optionsContainer = document.createElement('div');
        optionsContainer.className = 'options-container';
        optionsContainer.id = `${column}Options`;
        
        filterContainer.appendChild(selectAllDiv);
        filterContainer.appendChild(searchInput);
        filterContainer.appendChild(optionsContainer);
        
        filterCol.appendChild(label);
        filterCol.appendChild(filterContainer);

        // Add search functionality
        searchInput.addEventListener('input', (e) => {
            const searchText = e.target.value.toLowerCase();
            const options = optionsContainer.querySelectorAll('.form-check');
            options.forEach(option => {
                const text = option.textContent.toLowerCase();
                option.style.display = text.includes(searchText) ? '' : 'none';
            });
        });

        // Add select all functionality
        selectAllInput.addEventListener('change', (e) => {
            const options = optionsContainer.querySelectorAll('.form-check-input:not(.select-all)');
            options.forEach(option => {
                if (option.parentElement.style.display !== 'none') {
                    option.checked = e.target.checked;
                }
            });
            updateFilters(column);
        });
    }
    
    filterContainer.appendChild(filterCol);

    // Initialize filter state
    state.filters[column] = column === 'code' ? '' : [];
}

// Populate filters with values
function populateFilters(filterValues) {
    console.log('Populating filters with values...');
    state.filterOptions = filterValues;
    
    Object.entries(filterValues).forEach(([column, values]) => {
        const filter = document.getElementById(`${column}Filter`);
        if (!filter) {
            console.warn(`Filter element not found for column: ${column}`);
            return;
        }

        if (column === 'code') {
            // Format options to include both code and description
            const options = values.map(code => ({
                id: code,
                text: code // Initially just show the code, we'll update with descriptions later
            }));
            
            $(filter).empty().append('<option></option>');
            $(filter).select2({
                theme: 'bootstrap-5',
                width: '100%',
                placeholder: 'Search by code or description...',
                allowClear: true,
                data: options
            });
        } else {
            // Keep other filters empty initially
            filter.innerHTML = '<option value="">All</option>';
        }
    });
}

// Populate filters with values
function populateFilterOptions(column, values) {
    if (column === 'code') {
        const filter = document.getElementById(`${column}Filter`);
        if (!filter) return;

        const options = values.map(code => ({
            id: code,
            text: code
        }));
        
        $(filter).empty().append('<option></option>');
        $(filter).select2({
            theme: 'bootstrap-5',
            width: '100%',
            placeholder: 'Search by code or description...',
            allowClear: true,
            data: options
        });
    } else {
        const optionsContainer = document.getElementById(`${column}Options`);
        if (!optionsContainer) return;

        optionsContainer.innerHTML = '';
        values.forEach(value => {
            const optionDiv = document.createElement('div');
            optionDiv.className = 'form-check';
            
            const input = document.createElement('input');
            input.type = 'checkbox';
            input.className = 'form-check-input';
            input.id = `${column}-${value}`;
            input.value = value;
            
            const label = document.createElement('label');
            label.className = 'form-check-label';
            label.htmlFor = `${column}-${value}`;
            label.textContent = value;
            
            input.addEventListener('change', () => {
                updateFilters(column);
                updateSelectAllState(column);
            });
            
            optionDiv.appendChild(input);
            optionDiv.appendChild(label);
            optionsContainer.appendChild(optionDiv);
        });
    }
}

// Update filters based on checkbox selections
function updateFilters(column) {
    const optionsContainer = document.getElementById(`${column}Options`);
    if (!optionsContainer) return;

    const selectedValues = Array.from(optionsContainer.querySelectorAll('.form-check-input:checked'))
        .map(input => input.value);
    
    state.filters[column] = selectedValues;
    updateDependentFilters(column);
}

// Update select all checkbox state
function updateSelectAllState(column) {
    const selectAll = document.getElementById(`${column}SelectAll`);
    const options = document.getElementById(`${column}Options`);
    if (!selectAll || !options) return;

    const visibleCheckboxes = Array.from(options.querySelectorAll('.form-check-input'))
        .filter(input => input.parentElement.style.display !== 'none');
    const checkedCount = visibleCheckboxes.filter(input => input.checked).length;
    
    selectAll.checked = checkedCount === visibleCheckboxes.length;
    selectAll.indeterminate = checkedCount > 0 && checkedCount < visibleCheckboxes.length;
}

// Update dependent filters based on selected values
async function updateDependentFilters(changedFilter) {
    const filterIndex = FILTER_ORDER.indexOf(changedFilter);
    if (filterIndex === -1) return;

    // Clear all dependent filters
    for (let i = filterIndex + 1; i < FILTER_ORDER.length; i++) {
        const dependentFilter = FILTER_ORDER[i];
        if (dependentFilter !== 'code') {
            state.filters[dependentFilter] = [];
            const optionsContainer = document.getElementById(`${dependentFilter}Options`);
            if (optionsContainer) {
                optionsContainer.innerHTML = '';
            }
            const selectAll = document.getElementById(`${dependentFilter}SelectAll`);
            if (selectAll) {
                selectAll.checked = false;
                selectAll.indeterminate = false;
            }
        }
    }

    try {
        showLoading();
        // Build query parameters based on selected filters
        const queryParams = new URLSearchParams();
        for (let i = 0; i <= filterIndex; i++) {
            const filter = FILTER_ORDER[i];
            const values = state.filters[filter];
            if (Array.isArray(values)) {
                values.forEach(value => queryParams.append(filter, value));
            } else if (values) {
                queryParams.append(filter, values);
            }
        }

        const response = await fetch(`${API_ENDPOINT}?${queryParams}`);
        if (!response.ok) throw new Error('Failed to fetch dependent filter data');
        
        const result = await response.json();
        const data = result.data;

        if (!data || data.length === 0) {
            console.warn('No data returned for the selected filters');
            return;
        }

        // Update dependent filters with available values
        for (let i = filterIndex + 1; i < FILTER_ORDER.length; i++) {
            const dependentFilter = FILTER_ORDER[i];
            if (dependentFilter !== 'code') {
                const uniqueValues = [...new Set(data.map(item => item[dependentFilter]))].filter(Boolean).sort();
                populateFilterOptions(dependentFilter, uniqueValues);
            }
        }

    } catch (error) {
        console.error('Error updating dependent filters:', error);
        showError('Failed to update filters. Please try again.');
    } finally {
        hideLoading();
    }
}

// Update the table with filtered data
function updateTable() {
    if (!state.currentData.length) {
        reportTableBody.innerHTML = '<tr><td colspan="100%" class="text-center">No data found for the selected filters</td></tr>';
        return;
    }

    // Define the column mapping
    const columnMap = {
        'code': 'Code',
        'description': 'Description',
        'hospital_name': 'Hospital Name',
        'hospital_address': 'Hospital Address',
        'city': 'City',
        'region': 'Region',
        'payer_name': 'Payer Name',
        'plan_name': 'Plan Name',
        'standard_charge_min': 'Standard Charge Min',
        'standard_charge_max': 'Standard Charge Max',
        'standard_charge_gross': 'Standard Charge Gross',
        'standard_charge_negotiated_dollar': 'Standard Charge Negotiated'
    };

    // Setup table headers with sort buttons
    const headerRow = document.createElement('tr');
    Object.entries(columnMap).forEach(([key, headerText]) => {
        const th = document.createElement('th');
        const headerContent = document.createElement('div');
        headerContent.className = 'd-flex align-items-center justify-content-between';
        
        const textSpan = document.createElement('span');
        textSpan.textContent = headerText;
        
        const sortButton = document.createElement('button');
        sortButton.className = 'btn btn-sort';
        sortButton.innerHTML = `
            <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor" xmlns="http://www.w3.org/2000/svg">
                <path d="M8 3l4 4H4l4-4z"/>
                <path d="M8 13l-4-4h8l-4 4z"/>
            </svg>
        `;
        if (state.sortColumn === key) {
            sortButton.classList.add(state.sortDirection === 'asc' ? 'sort-asc' : 'sort-desc');
        }
        
        sortButton.addEventListener('click', () => {
            const newDirection = state.sortColumn === key && state.sortDirection === 'asc' ? 'desc' : 'asc';
            state.sortColumn = key;
            state.sortDirection = newDirection;
            sortData();
            updateTable();
        });
        
        headerContent.appendChild(textSpan);
        headerContent.appendChild(sortButton);
        th.appendChild(headerContent);
        headerRow.appendChild(th);
    });
    
    tableHeader.innerHTML = '';
    tableHeader.appendChild(headerRow);

    // Use DocumentFragment for better performance
    const fragment = document.createDocumentFragment();
    state.currentData.forEach(item => {
        const row = document.createElement('tr');
        Object.keys(columnMap).forEach(key => {
            const td = document.createElement('td');
            const value = item[key];
            td.textContent = formatValue(value);
            if (typeof value === 'number' || (value && !isNaN(value))) {
                td.className = 'text-end';
            }
            row.appendChild(td);
        });
        fragment.appendChild(row);
    });

    reportTableBody.innerHTML = '';
    reportTableBody.appendChild(fragment);
}

// Sort data function
function sortData() {
    if (!state.sortColumn) return;

    state.currentData.sort((a, b) => {
        let aVal = a[state.sortColumn];
        let bVal = b[state.sortColumn];

        // Handle null/undefined values
        if (aVal === null || aVal === undefined) aVal = '';
        if (bVal === null || bVal === undefined) bVal = '';

        // Convert to numbers if possible
        if (typeof aVal === 'string' && !isNaN(aVal)) aVal = parseFloat(aVal);
        if (typeof bVal === 'string' && !isNaN(bVal)) bVal = parseFloat(bVal);

        // Compare values
        if (aVal < bVal) return state.sortDirection === 'asc' ? -1 : 1;
        if (aVal > bVal) return state.sortDirection === 'asc' ? 1 : -1;
        return 0;
    });
}

// Setup event listeners
function setupEventListeners() {
    const debouncedFetchData = debounce(async () => {
        await fetchData(1);
        updateTable();
    }, DEBOUNCE_DELAY);

    FILTER_ORDER.forEach(column => {
        if (column === 'code') {
            const filter = document.getElementById(`${column}Filter`);
            if (filter) {
                $(filter).on('select2:select select2:clear', async (e) => {
                    state.filters[column] = e.type === 'select2:clear' ? '' : e.params?.data?.id || '';
                    await updateDependentFilters(column);
                    await debouncedFetchData();
                });
            }
        }
    });

    // Add global change event listener for checkbox changes
    document.addEventListener('change', async (e) => {
        if (e.target.matches('.form-check-input') && !e.target.matches('.select-all')) {
            const column = e.target.id.split('-')[0];
            if (FILTER_ORDER.includes(column) && column !== 'code') {
                await debouncedFetchData();
            }
        }
    });
}

// Update pagination controls
function updatePagination() {
    const maxVisiblePages = 5;
    let startPage = Math.max(1, state.currentPage - Math.floor(maxVisiblePages / 2));
    let endPage = Math.min(state.totalPages, startPage + maxVisiblePages - 1);

    if (endPage - startPage + 1 < maxVisiblePages) {
        startPage = Math.max(1, endPage - maxVisiblePages + 1);
    }

    const pageNumbers = [];
    for (let i = startPage; i <= endPage; i++) {
        pageNumbers.push(`
            <button class="btn ${i === state.currentPage ? 'btn-primary' : 'btn-secondary'}"
                    onclick="changePage(${i})"
                    ${i === state.currentPage ? 'disabled' : ''}>
                ${i}
            </button>
        `);
    }

    paginationContainer.innerHTML = `
        <div class="d-flex gap-2 align-items-center">
            <button class="btn btn-secondary" onclick="changePage(1)" ${state.currentPage === 1 ? 'disabled' : ''}>
                <i class="bi bi-chevron-double-left"></i>
            </button>
            <button class="btn btn-secondary" onclick="changePage(${state.currentPage - 1})" ${state.currentPage === 1 ? 'disabled' : ''}>
                <i class="bi bi-chevron-left"></i>
            </button>
            ${pageNumbers.join('')}
            <button class="btn btn-secondary" onclick="changePage(${state.currentPage + 1})" ${state.currentPage === state.totalPages ? 'disabled' : ''}>
                <i class="bi bi-chevron-right"></i>
            </button>
            <button class="btn btn-secondary" onclick="changePage(${state.totalPages})" ${state.currentPage === state.totalPages ? 'disabled' : ''}>
                <i class="bi bi-chevron-double-right"></i>
            </button>
        </div>
        <div class="pagination-info">
            Page ${state.currentPage} of ${state.totalPages}
        </div>
    `;
}

// Change page
async function changePage(page) {
    if (page < 1 || page > state.totalPages) return;
    await fetchData(page);
    updateTable();
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
    
    if (typeof value === 'number' || (typeof value === 'string' && !isNaN(value))) {
        const num = parseFloat(value);
        if (String(value).includes('.') || 
            String(value).toLowerCase().includes('charge') || 
            String(value).toLowerCase().includes('price')) {
            return new Intl.NumberFormat('en-US', { 
                style: 'currency', 
                currency: 'USD',
                minimumFractionDigits: 2,
                maximumFractionDigits: 2
            }).format(num);
        }
        return new Intl.NumberFormat('en-US', {
            minimumFractionDigits: 0,
            maximumFractionDigits: 2
        }).format(num);
    }
    
    return String(value);
} 