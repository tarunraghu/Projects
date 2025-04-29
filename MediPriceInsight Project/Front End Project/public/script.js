// Global variables
let currentFilters = {
    state: [],
    city: [],
    hospital: [],
    procedure: [],
    insurance: []
};

const filterOrder = ['state', 'city', 'hospital', 'procedure', 'insurance'];
const filterLabels = {
    state: 'State',
    city: 'City',
    hospital: 'Hospital',
    procedure: 'Procedure',
    insurance: 'Insurance'
};

// Initialize filters and table
document.addEventListener('DOMContentLoaded', async function() {
    await initializeFilters();
    updateTable();
});

async function initializeFilters() {
    try {
        const response = await fetch('/api/filters');
        const data = await response.json();
        
        // Create filters in the specified order
        filterOrder.forEach(filterType => {
            if (data[filterType]) {
                createFilter(filterType, data[filterType]);
            }
        });

        // Initialize Select2 for all select elements
        $('.filter-select').each(function() {
            $(this).select2({
                theme: 'bootstrap-5',
                width: '100%',
                placeholder: `Select ${$(this).data('placeholder')}`,
                allowClear: true
            });
        });

        // Add change event listeners
        $('.filter-select').on('change', function() {
            const filterType = $(this).data('filter-type');
            currentFilters[filterType] = $(this).val() || [];
            updateActiveFilters();
            updateDependentFilters(filterType);
            updateTable();
        });

    } catch (error) {
        console.error('Error initializing filters:', error);
    }
}

function createFilter(filterType, options) {
    const col = document.createElement('div');
    col.className = 'col-md-2 mb-3';
    
    const select = document.createElement('select');
    select.className = 'filter-select form-select';
    select.setAttribute('data-filter-type', filterType);
    select.setAttribute('data-placeholder', filterLabels[filterType]);
    select.multiple = true;

    options.forEach(option => {
        const optElement = document.createElement('option');
        optElement.value = option;
        optElement.textContent = option;
        select.appendChild(optElement);
    });

    col.appendChild(select);
    document.getElementById('filterContainer').appendChild(col);
}

function updateActiveFilters() {
    const activeFiltersContainer = document.getElementById('activeFilters');
    activeFiltersContainer.innerHTML = '';

    filterOrder.forEach(filterType => {
        if (currentFilters[filterType] && currentFilters[filterType].length > 0) {
            currentFilters[filterType].forEach(value => {
                const chip = document.createElement('div');
                chip.className = 'filter-chip';
                chip.innerHTML = `
                    <span class="filter-name">${filterLabels[filterType]}:</span>
                    <span class="filter-value">${value}</span>
                    <span class="close" data-filter-type="${filterType}" data-value="${value}">&times;</span>
                `;
                activeFiltersContainer.appendChild(chip);

                // Add click handler for removing the filter
                chip.querySelector('.close').addEventListener('click', function() {
                    const filterType = this.dataset.filterType;
                    const value = this.dataset.value;
                    removeFilter(filterType, value);
                });
            });
        }
    });
}

function removeFilter(filterType, value) {
    const select = $(`.filter-select[data-filter-type="${filterType}"]`);
    const values = select.val().filter(v => v !== value);
    select.val(values).trigger('change');
}

async function updateDependentFilters(changedFilterType) {
    try {
        const response = await fetch('/api/dependent-filters', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(currentFilters)
        });

        const data = await response.json();

        // Update each filter's options based on the response
        filterOrder.forEach(filterType => {
            if (filterType !== changedFilterType && data[filterType]) {
                const select = $(`.filter-select[data-filter-type="${filterType}"]`);
                const currentValue = select.val();

                // Store current values that are still valid
                const validValues = currentValue ? 
                    currentValue.filter(value => data[filterType].includes(value)) : 
                    [];

                // Update options
                select.empty();
                data[filterType].forEach(option => {
                    select.append(new Option(option, option, false, validValues.includes(option)));
                });

                // Update the currentFilters object and trigger change
                select.val(validValues).trigger('change');
            }
        });

    } catch (error) {
        console.error('Error updating dependent filters:', error);
    }
}

async function updateTable() {
    const loadingSpinner = document.getElementById('loadingSpinner');
    const tableContainer = document.getElementById('tableContainer');
    
    try {
        loadingSpinner.style.display = 'block';
        tableContainer.style.opacity = '0.5';

        const response = await fetch('/api/filtered-data', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(currentFilters)
        });

        const data = await response.json();
        
        // Clear existing table
        const tableBody = document.querySelector('#dataTable tbody');
        tableBody.innerHTML = '';

        // Add new rows
        data.forEach(row => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>${row.hospital}</td>
                <td>${row.procedure}</td>
                <td>${row.insurance}</td>
                <td>${row.cost}</td>
                <td>${row.city}</td>
                <td>${row.state}</td>
            `;
            tableBody.appendChild(tr);
        });

    } catch (error) {
        console.error('Error updating table:', error);
    } finally {
        loadingSpinner.style.display = 'none';
        tableContainer.style.opacity = '1';
    }
} 