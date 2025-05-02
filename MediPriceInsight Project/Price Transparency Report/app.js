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
    filteredData: [],
    allData: [],
    filters: {},
    filterOptions: {},
    currentPage: 1,
    perPage: 100,
    totalPages: 1,
    isLoading: false,
    lastFetchTime: 0,
    sortColumn: null,
    sortDirection: 'asc',
    codeDescriptionMap: {},
    descriptionsLoaded: false
};

// Initialize the page
document.addEventListener('DOMContentLoaded', async () => {
    try {
        // Wrap the table in a container
        const tableResponsive = document.querySelector('.table-responsive');
        const tableContainer = document.createElement('div');
        tableContainer.className = 'table-container';
        tableResponsive.parentNode.insertBefore(tableContainer, tableResponsive);
        tableContainer.appendChild(tableResponsive);

        // Add filter-container class
        const filterContainer = document.getElementById('filterContainer');
        filterContainer.className = 'filter-container row';

        // Add dropdown state management
        document.addEventListener('show.bs.dropdown', function(e) {
            const filterContainer = e.target.closest('.filter-container');
            if (filterContainer) {
                filterContainer.style.zIndex = '1500';
            }
        });

        document.addEventListener('hidden.bs.dropdown', function(e) {
            const filterContainer = e.target.closest('.filter-container');
            if (filterContainer) {
                filterContainer.style.zIndex = '1';
            }
        });

        showLoading();
        await setupFilters();
        setupEventListeners();
        hideLoading();
        
        reportTableBody.innerHTML = '<tr><td colspan="100%" class="text-center">Please select a code to view data</td></tr>';
    } catch (error) {
        console.error('Error during initialization:', error);
        showError('Failed to initialize the application. Please try again later.');
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
        console.log('Starting filter setup...');
        
        // First fetch distinct codes and descriptions
        console.log('Fetching codes and descriptions...');
        const codesResponse = await fetch(`${API_ENDPOINT}/codes`);
        if (!codesResponse.ok) throw new Error('Failed to fetch codes');
        const codesData = await codesResponse.json();
        
        console.log('Received codes data:', {
            count: codesData.data?.length || 0
        });

        // Create filter values object with codes
        const filterValues = {
            code: [],
            region: [],
            city: [],
            payer_name: [],
            plan_name: []
        };

        // Create uniqueCodes map with descriptions
        const uniqueCodes = new Map();
        if (codesData.data) {
            codesData.data.forEach(item => {
                if (item.code) {
                    uniqueCodes.set(item.code, {
                        code: item.code,
                        description: item.description || ''
                    });
                    filterValues.code.push(item.code);
                }
            });
        }

        // Sort codes alphanumerically
        filterValues.code.sort((a, b) => {
            // Extract numeric and non-numeric parts
            const aMatch = a.match(/^(\D+)?(\d+)?(\D+)?(\d+)?/);
            const bMatch = b.match(/^(\D+)?(\d+)?(\D+)?(\d+)?/);
            
            if (!aMatch || !bMatch) return a.localeCompare(b);
            
            // Compare parts
            for (let i = 1; i < 5; i++) {
                const aPart = aMatch[i] || '';
                const bPart = bMatch[i] || '';
                
                if (i % 2 === 0) {
                    // Compare numeric parts
                    const aNum = parseInt(aPart || '0', 10);
                    const bNum = parseInt(bPart || '0', 10);
                    if (aNum !== bNum) return aNum - bNum;
                } else {
                    // Compare non-numeric parts
                    if (aPart !== bPart) return aPart.localeCompare(bPart);
                }
            }
            return 0;
        });

        // Setup UI with codes and descriptions
        setupDynamicFilters(Object.keys(filterValues));
        populateFilters(filterValues, uniqueCodes);

        // Add event listener for code selection
        const codeFilter = document.getElementById('codeFilter');
        if (codeFilter) {
            $(codeFilter).on('select2:select', async function(e) {
                const selectedCode = e.params.data.id;
                if (selectedCode) {
                    showLoading();
                    try {
                        // Fetch full data for the selected code
                        const response = await fetch(`${API_ENDPOINT}?code=${selectedCode}`);
                        if (!response.ok) throw new Error('Failed to fetch data');
                        const data = await response.json();
                        
                        // Update state with the fetched data
                        state.allData = data.data || [];
                        
                        // Extract unique values for other filters
                        const uniqueValues = {
                            region: new Set(),
                            city: new Set(),
                            payer_name: new Set(),
                            plan_name: new Set()
                        };
                        
                        state.allData.forEach(item => {
                            if (item.region) uniqueValues.region.add(String(item.region));
                            if (item.city) uniqueValues.city.add(String(item.city));
                            if (item.payer_name) uniqueValues.payer_name.add(String(item.payer_name));
                            if (item.plan_name) uniqueValues.plan_name.add(String(item.plan_name));
                        });
                        
                        // Update filter values
                        Object.keys(uniqueValues).forEach(key => {
                            filterValues[key] = Array.from(uniqueValues[key]).sort();
                        });
                        
                        // Update other dropdowns
                        updateDependentFilters('code');
                        applyFilters();
                        updateTable();
                    } catch (error) {
                        console.error('Error fetching data:', error);
                        showError('Failed to load data for the selected code');
                    } finally {
                        hideLoading();
                    }
                }
            });

            // Handle clear event
            $(codeFilter).on('select2:clear', function() {
                // Reset all data and filters
                state.allData = [];
                state.filteredData = [];
                state.currentData = [];
                
                // Reset other filter values
                ['region', 'city', 'payer_name', 'plan_name'].forEach(key => {
                    filterValues[key] = [];
                    const filter = $(`#${key}Filter`);
                    if (filter.length) {
                        filter.val(null).trigger('change');
                    }
                });
                
                // Update table
                reportTableBody.innerHTML = '<tr><td colspan="100%" class="text-center">Please select a code to view data</td></tr>';
            });
        }

    } catch (error) {
        console.error('Error in setupFilters:', error);
        hideLoading();
        showError('Failed to load codes and descriptions');
        throw error;
    }
}

// Function to load remaining descriptions in the background
async function loadRemainingDescriptions(codes, uniqueCodes) {
    const batchSize = 10;
    const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

    for (let i = 0; i < codes.length; i += batchSize) {
        const batch = codes.slice(i, i + batchSize);
        const batchPromises = batch.map(async code => {
            try {
                const response = await fetch(`${API_ENDPOINT}?code=${code}`);
                if (!response.ok) return { code, description: '' };
                const data = await response.json();
                return {
                    code,
                    description: data.data?.[0]?.description || ''
                };
            } catch (error) {
                console.warn(`Failed to fetch description for code ${code}:`, error);
                return { code, description: '' };
            }
        });

        const results = await Promise.all(batchPromises);
        results.forEach(({ code, description }) => {
            if (uniqueCodes.has(code)) {
                uniqueCodes.get(code).description = description;
            }
        });

        // Update the dropdown with new descriptions
        const codeFilter = document.getElementById('codeFilter');
        if (codeFilter) {
            const options = $(codeFilter).select2('data');
            options.forEach(option => {
                if (uniqueCodes.has(option.code)) {
                    const description = uniqueCodes.get(option.code).description;
                    option.text = description ? `${option.code} - ${description}` : option.code;
                }
            });
            $(codeFilter).trigger('change');
        }

        // Add a small delay between batches to prevent overwhelming the server
        await delay(100);
    }
}

// Fetch data from the backend
async function fetchData(page = 1, isInitialLoad = false) {
    const now = Date.now();
    if (!isInitialLoad && now - state.lastFetchTime < DEBOUNCE_DELAY) {
        console.log('Throttling API call');
        return;
    }
    
    try {
        showLoading();
        console.log('Fetching data from API...');
        
        if (isInitialLoad) {
            // On initial load, fetch all data for the selected code
            const params = new URLSearchParams();
            params.append('code', state.filters.code);
            
            const response = await fetch(`${API_ENDPOINT}?${params}`);
            
            if (!response.ok) throw new Error('Failed to fetch data');
            const result = await response.json();
            state.allData = result.data;
            console.log('Fetched data count:', state.allData.length);
            applyFilters();
        }
        
        state.lastFetchTime = now;
        updatePagination();
        return { data: state.filteredData };
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
    filterCol.className = 'col-md-4 mb-3 filter-row';
    
    const label = document.createElement('label');
    label.className = 'form-label';
    label.textContent = column === 'code' ? 'Code or Description *' : formatColumnName(column);
    label.htmlFor = `${column}Filter`;

    // Create select element for all filters
    const select = document.createElement('select');
    select.className = 'form-select';
    select.id = `${column}Filter`;
    if (column !== 'code') {
        select.multiple = true;
    }
    
    filterCol.appendChild(label);
    filterCol.appendChild(select);

    // Initialize Select2 with appropriate configuration
    $(select).select2({
        theme: 'bootstrap-5',
        width: '100%',
        placeholder: column === 'code' ? 'Search by code...' : `Select ${formatColumnName(column)}...`,
        allowClear: true,
        multiple: column !== 'code',
        closeOnSelect: column === 'code',
        selectionCssClass: 'select2--small',
        dropdownCssClass: 'select2--small',
        templateResult: function(data) {
            if (!data.id) return data.text;
            return $('<span>').text(data.text);
        }
    });

    // Add event listeners for Select2
    if (column !== 'code') {
        $(select).on('select2:select select2:unselect', function(e) {
            const values = $(this).val() || [];
            state.filters[column] = values;
            updateFilters(column);
        });
    }

    filterContainer.appendChild(filterCol);
    state.filters[column] = column === 'code' ? '' : [];
}

// Populate filters with values
function populateFilters(filterValues, uniqueCodes) {
    console.log('Starting filter population...');
    state.filterOptions = filterValues;
    
    Object.entries(filterValues).forEach(([column, values]) => {
        const filter = document.getElementById(`${column}Filter`);
        if (!filter) {
            console.warn(`Filter element not found for column: ${column}`);
            return;
        }

        if (column === 'code') {
            console.log(`Populating ${values.length} codes with descriptions...`);
            
            // Format options to include both code and description
            const options = values.map(code => {
                const codeData = uniqueCodes.get(code);
                const description = codeData?.description || '';
                return {
                    id: code,
                    text: description ? `${code} - ${description}` : code,
                    code: code,
                    description: description
                };
            });

            console.log('Sample formatted options:', options.slice(0, 5));
            
            $(filter).empty().append('<option></option>');
            $(filter).select2({
                theme: 'bootstrap-5',
                width: '100%',
                placeholder: 'Search by code or description...',
                allowClear: true,
                data: options,
                matcher: function(params, data) {
                    // If there are no search terms, return all of the data
                    if ($.trim(params.term) === '') {
                        return data;
                    }

                    // Search in both code and description, case insensitive
                    const searchTerm = params.term.toLowerCase();
                    const code = data.code.toLowerCase();
                    const description = data.description.toLowerCase();
                    const text = data.text.toLowerCase();

                    // Match if the search term appears in code, description, or full text
                    if (code.includes(searchTerm) || 
                        description.includes(searchTerm) || 
                        text.includes(searchTerm)) {
                        return data;
                    }

                    // Return null if no match
                    return null;
                }
            });
        } else {
            // For other filters, initialize with empty state
            $(filter).empty().append('<option></option>');
            $(filter).select2({
                theme: 'bootstrap-5',
                width: '100%',
                placeholder: `Select ${formatColumnName(column)}...`,
                allowClear: true,
                multiple: true,
                data: []  // Start with empty data
            });
        }
    });
}

// Populate filter options
function populateFilterOptions(column, values) {
    const filter = document.getElementById(`${column}Filter`);
    if (!filter) return;

    let options;
    if (column === 'code') {
        // Get unique code-description pairs with counts
        const codeDescriptionPairs = state.allData.reduce((pairs, item) => {
            if (item.code) {
                if (!pairs.has(item.code)) {
                    pairs.set(item.code, {
                        code: item.code,
                        description: item.description || '',
                        count: 1
                    });
                } else {
                    const pair = pairs.get(item.code);
                    pair.count++;
                }
            }
            return pairs;
        }, new Map());

        options = Array.from(codeDescriptionPairs.values())
            .map(({ code, description, count }) => ({
                id: code,
                text: description ? `${code} - ${description} (${count})` : `${code} (${count})`,
                count: count
            }))
            .sort((a, b) => a.id.localeCompare(b.id));
    } else {
        // Count occurrences for other filters
        const valueCounts = values.reduce((counts, value) => {
            if (value !== null && value !== '') {
                const stringValue = String(value);
                // Filter the data based on current selections
                const filteredData = state.allData.filter(item => {
                    for (const [filterName, filterValue] of Object.entries(state.filters)) {
                        if (filterName === column) continue;
                        if (filterValue && (
                            (Array.isArray(filterValue) && filterValue.length > 0 && !filterValue.includes(String(item[filterName]))) ||
                            (!Array.isArray(filterValue) && filterValue !== String(item[filterName]))
                        )) {
                            return false;
                        }
                    }
                    return true;
                });
                
                const count = filteredData.filter(item => String(item[column]) === stringValue).length;
                counts.set(stringValue, count);
            }
            return counts;
        }, new Map());

        options = Array.from(valueCounts.entries())
            .map(([value, count]) => ({
                id: value,
                text: `${value} (${count})`,
                count: count
            }))
            .sort((a, b) => a.id.localeCompare(b.id));
    }
    
    $(filter).empty();
    $(filter).select2({
        theme: 'bootstrap-5',
        width: '100%',
        placeholder: column === 'code' ? 'Search by code...' : `Select ${formatColumnName(column)}...`,
        allowClear: true,
        multiple: column !== 'code',
        closeOnSelect: column === 'code',
        data: options,
        templateResult: function(data) {
            if (!data.id) return data.text; // Skip placeholder
            return $('<span>').html(data.text);
        },
        templateSelection: function(data) {
            if (!data.id) return data.text; // Skip placeholder
            
            // For selected items, show with count
            if (column === 'code') {
                // For code, show code - description (count)
                return data.text;
            } else {
                // For other filters, show value (count)
                const option = options.find(opt => opt.id === data.id);
                return option ? `${option.id} (${option.count})` : data.id;
            }
        }
    });

    // Style the selected items to match the dropdown style
    const style = document.createElement('style');
    style.textContent = `
        .select2-selection__choice {
            background-color: #673ab7 !important;
            color: white !important;
            border: none !important;
            padding: 2px 8px !important;
        }
        
        .select2-selection__choice__display {
            color: white !important;
            padding: 0 !important;
        }
        
        .select2-selection__choice__remove {
            color: white !important;
            border: none !important;
            background: transparent !important;
            padding: 0 4px !important;
            margin-right: 4px !important;
        }
        
        .select2-selection__choice__remove:hover {
            background-color: rgba(255, 255, 255, 0.2) !important;
            color: #e0e0e0 !important;
        }
    `;
    document.head.appendChild(style);
}

// Update filters based on checkbox selections
function updateFilters(column) {
    if (column === 'code') return;

    const optionsContainer = document.getElementById(`${column}Options`);
    if (!optionsContainer) return;

    const selectedValues = Array.from(optionsContainer.querySelectorAll('.form-check-input:checked:not(.select-all)'))
        .map(input => input.value);
    
    state.filters[column] = selectedValues;
    updateSelectedText(column);
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

    try {
        // Get current filtered data based on code selection
        let filteredData = [...state.allData];
        
        // Filter data based on selected code first
        const selectedCode = state.filters.code;
        if (selectedCode) {
            filteredData = filteredData.filter(item => item.code === selectedCode);
        }
        
        // Special handling for region-city dependency
        if (changedFilter === 'region') {
            const selectedRegions = state.filters.region || [];
            if (selectedRegions.length > 0) {
                // Filter cities based on selected regions
                filteredData = filteredData.filter(item => selectedRegions.includes(String(item.region)));
                const uniqueCities = [...new Set(filteredData.map(item => String(item.city)))].filter(Boolean);
                
                // Get counts for cities
                const cityCounts = uniqueCities.map(city => ({
                    id: city,
                    count: filteredData.filter(item => String(item.city) === city).length
                }));
                
                // Update city filter with counts
                const cityOptions = cityCounts
                    .map(({ id, count }) => ({
                        id,
                        text: `${id} (${count})`,
                        count: count
                    }))
                    .sort((a, b) => a.id.localeCompare(b.id));

                const cityFilter = $('#cityFilter');
                const currentSelectedCities = cityFilter.val() || [];
                const validSelectedCities = currentSelectedCities.filter(city => 
                    uniqueCities.includes(city)
                );

                cityFilter.empty();
                cityFilter.select2({
                    theme: 'bootstrap-5',
                    width: '100%',
                    placeholder: 'Select City...',
                    allowClear: true,
                    multiple: true,
                    data: cityOptions,
                    templateResult: function(data) {
                        if (!data.id) return data.text;
                        return $('<span>').html(data.text);
                    },
                    templateSelection: function(data) {
                        if (!data.id) return data.text;
                        const option = cityOptions.find(opt => opt.id === data.id);
                        return option ? `${option.id} (${option.count})` : data.id;
                    }
                });

                if (validSelectedCities.length > 0) {
                    cityFilter.val(validSelectedCities).trigger('change');
                }
                state.filters.city = validSelectedCities;

                // Update payer_name options with counts
                const uniquePayerNames = [...new Set(filteredData.map(item => String(item.payer_name)))].filter(Boolean);
                const payerCounts = uniquePayerNames.map(payer => ({
                    id: payer,
                    count: filteredData.filter(item => String(item.payer_name) === payer).length
                }));
                
                updateFilterOptions('payer_name', payerCounts.map(p => p.id));
            } else {
                resetDependentFilters(['city', 'payer_name', 'plan_name']);
                filteredData = state.allData.filter(item => item.code === selectedCode);
            }
        }

        // Update payer_name based on region and city selections
        if (changedFilter === 'city' || changedFilter === 'region') {
            let currentData = filteredData;
            
            // Apply region filter
            const selectedRegions = state.filters.region || [];
            if (selectedRegions.length > 0) {
                currentData = currentData.filter(item => selectedRegions.includes(String(item.region)));
            }
            
            // Apply city filter
            const selectedCities = state.filters.city || [];
            if (selectedCities.length > 0) {
                currentData = currentData.filter(item => selectedCities.includes(String(item.city)));
            }
            
            // Update payer_name options
            const uniquePayerNames = [...new Set(currentData.map(item => String(item.payer_name)))].filter(Boolean).sort();
            updateFilterOptions('payer_name', uniquePayerNames);
            
            // Reset plan_name as it depends on payer_name
            resetDependentFilters(['plan_name']);
        }

        // Update plan_name based on region, city, and payer_name selections
        if (changedFilter === 'payer_name' || changedFilter === 'city' || changedFilter === 'region') {
            let currentData = filteredData;
            
            // Apply all previous filters
            const selectedRegions = state.filters.region || [];
            if (selectedRegions.length > 0) {
                currentData = currentData.filter(item => selectedRegions.includes(String(item.region)));
            }
            
            const selectedCities = state.filters.city || [];
            if (selectedCities.length > 0) {
                currentData = currentData.filter(item => selectedCities.includes(String(item.city)));
            }
            
            const selectedPayerNames = state.filters.payer_name || [];
            if (selectedPayerNames.length > 0) {
                currentData = currentData.filter(item => selectedPayerNames.includes(String(item.payer_name)));
            }
            
            // Update plan_name options
            const uniquePlanNames = [...new Set(currentData.map(item => String(item.plan_name)))].filter(Boolean).sort();
            updateFilterOptions('plan_name', uniquePlanNames);
        }

        // Apply filters and update table
        applyFilters();
        updateTable();

    } catch (error) {
        console.error('Error updating dependent filters:', error);
        showError('Failed to update filters. Please try again.');
    }
}

// Helper function to update filter options
function updateFilterOptions(filterName, values) {
    const filter = $(`#${filterName}Filter`);
    const currentSelected = filter.val() || [];
    
    // Only keep currently selected values that are still valid
    const validSelected = currentSelected.filter(value => values.includes(value));
    
    // Update options and selection
    populateFilterOptions(filterName, values);
    if (validSelected.length > 0) {
        filter.val(validSelected).trigger('change');
    } else {
        filter.val(null).trigger('change');
    }
    
    // Update state
    state.filters[filterName] = validSelected;
}

// Helper function to reset dependent filters
function resetDependentFilters(filterNames) {
    filterNames.forEach(filterName => {
        const filter = $(`#${filterName}Filter`);
        if (filter.length) {
            filter.val(null).trigger('change');
            state.filters[filterName] = [];
        }
    });
}

// Update the table with filtered data
function updateTable(data) {
    // Define the columns you want to show
    const columns = [
        { key: 'hospital_name', label: 'Hospital Name' },
        { key: 'code', label: 'Code' },
        { key: 'description', label: 'Description' },
        { key: 'city', label: 'City' },
        { key: 'region', label: 'Region' },
        { key: 'payer_name', label: 'Payer Name' },
        { key: 'plan_name', label: 'Plan Name' },
        { key: 'standard_charge_min', label: 'Standard Charge Min' },
        { key: 'standard_charge_max', label: 'Standard Charge Max' },
        { key: 'standard_charge_negotiated_dollar', label: 'Standard Charge Negotiated' }
    ];

    // Table sorting state
    if (!window.tableSortState) {
        window.tableSortState = { column: null, direction: 'asc' };
    }

    // Sort data if needed
    if (window.tableSortState.column) {
        const colKey = window.tableSortState.column;
        const dir = window.tableSortState.direction;
        data = [...data].sort((a, b) => {
            let aVal = a[colKey] || '';
            let bVal = b[colKey] || '';
            // Try numeric sort if both are numbers
            if (!isNaN(aVal) && !isNaN(bVal) && aVal !== '' && bVal !== '') {
                aVal = parseFloat(aVal);
                bVal = parseFloat(bVal);
            }
            if (aVal < bVal) return dir === 'asc' ? -1 : 1;
            if (aVal > bVal) return dir === 'asc' ? 1 : -1;
            return 0;
        });
    }

    // Update thead
    const thead = document.getElementById('tableHeader');
    thead.innerHTML = '';
    const headerRow = document.createElement('tr');
    columns.forEach(col => {
        const th = document.createElement('th');
        th.style.verticalAlign = 'middle';
        const labelSpan = document.createElement('span');
        labelSpan.textContent = col.label;
        th.appendChild(labelSpan);
        // Add sort button
        const sortBtn = document.createElement('button');
        sortBtn.className = 'btn btn-link btn-sm p-0 ms-1';
        sortBtn.innerHTML =
            window.tableSortState.column === col.key
                ? (window.tableSortState.direction === 'asc'
                    ? '<span style="font-size:1.1em">▲</span>'
                    : '<span style="font-size:1.1em">▼</span>')
                : '<span style="font-size:1.1em;color:#bbb">⇅</span>';
        sortBtn.onclick = () => {
            if (window.tableSortState.column === col.key) {
                window.tableSortState.direction = window.tableSortState.direction === 'asc' ? 'desc' : 'asc';
            } else {
                window.tableSortState.column = col.key;
                window.tableSortState.direction = 'asc';
            }
            updateTable(data);
        };
        th.appendChild(sortBtn);
        headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);

    // Add unique count row
    const uniqueRow = document.createElement('tr');
    columns.forEach(col => {
        const uniqueVals = new Set(data.map(row => row[col.key] ?? ''));
        const td = document.createElement('td');
        td.style.textAlign = 'center';
        td.style.fontSize = '0.9em';
        td.style.color = '#673ab7';
        td.innerHTML = `<strong>${uniqueVals.size}</strong> <span style="font-size:0.8em;color:#888;">unique</span>`;
        uniqueRow.appendChild(td);
    });
    thead.appendChild(uniqueRow);

    // Update tbody
    const tbody = document.getElementById('reportTableBody');
    tbody.innerHTML = '';
    if (!data.length) {
        tbody.innerHTML = '<tr><td colspan="10" class="text-center">No data found for the selected filters</td></tr>';
        return;
    }
    data.forEach(item => {
        const row = document.createElement('tr');
        columns.forEach(col => {
            const td = document.createElement('td');
            let value = item[col.key] || '';
            if ([
                'standard_charge_min',
                'standard_charge_max',
                'standard_charge_negotiated_dollar'
            ].includes(col.key) && value !== '') {
                value = '$' + value;
                td.className = 'text-end';
            }
            td.textContent = value;
            row.appendChild(td);
        });
        tbody.appendChild(row);
    });
    $('.table-responsive').show();
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
    FILTER_ORDER.forEach(column => {
        if (column === 'code') {
            const filter = document.getElementById(`${column}Filter`);
            if (filter) {
                $(filter).on('select2:select select2:clear', async (e) => {
                    state.filters[column] = e.type === 'select2:clear' ? '' : e.params?.data?.id || '';
                    
                    if (e.type === 'select2:clear') {
                        // Clear everything
                        state.currentData = [];
                        state.filteredData = [];
                        state.allData = [];
                        reportTableBody.innerHTML = '<tr><td colspan="100%" class="text-center">Please select a code to view data</td></tr>';
                        resetDependentFilters(['region', 'city', 'payer_name', 'plan_name']);
                    } else {
                        // Fetch all data for the selected code
                        await fetchData(1, true);
                        
                        // Update region options based on the selected code
                        const uniqueRegions = [...new Set(state.allData.map(item => String(item.region)))].filter(Boolean).sort();
                        updateFilterOptions('region', uniqueRegions);
                        
                        // Reset other dependent filters
                        resetDependentFilters(['city', 'payer_name', 'plan_name']);
                        
                        updateTable();
                    }
                });
            }
        } else {
            const filter = document.getElementById(`${column}Filter`);
            if (filter) {
                $(filter).on('select2:select select2:unselect', async function(e) {
                    const values = $(this).val() || [];
                    state.filters[column] = values;
                    await updateDependentFilters(column);
                });
            }
        }
    });

    // Update checkbox change event listener for instant filtering
    document.addEventListener('change', async (e) => {
        if (e.target.matches('.form-check-input') && !e.target.matches('.select-all')) {
            const column = e.target.id.split('-')[0];
            if (FILTER_ORDER.includes(column) && column !== 'code' && state.filters.code) {
                updateFilters(column);
                applyFilters();
                updateTable();
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
function changePage(page) {
    if (page < 1 || page > state.totalPages) return;
    state.currentPage = page;
    applyFilters();
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
function formatValue(value, columnName = '') {
    if (value === null || value === undefined) return '';
    
    // Handle code values without any numeric formatting
    if (columnName === 'code') {
        return String(value);
    }

    // Handle monetary values
    if (typeof value === 'number' || (typeof value === 'string' && !isNaN(value))) {
        const num = parseFloat(value);
        
        // Format monetary values
        if (columnName.toLowerCase().includes('charge') || 
            columnName.toLowerCase().includes('price') || 
            String(value).includes('.')) {
            return new Intl.NumberFormat('en-US', { 
                style: 'currency', 
                currency: 'USD',
                minimumFractionDigits: 2,
                maximumFractionDigits: 2
            }).format(num);
        }
        
        // Format other numeric values
        return new Intl.NumberFormat('en-US', {
            minimumFractionDigits: 0,
            maximumFractionDigits: 2
        }).format(num);
    }
    
    return String(value);
}

// Update selected text in dropdown button
function updateSelectedText(column) {
    if (column === 'code') return;

    const dropdown = document.getElementById(`${column}Dropdown`);
    if (!dropdown) return;

    const selectedValues = state.filters[column];
    const selectedText = dropdown.querySelector('.selected-text');
    
    if (!selectedValues || selectedValues.length === 0) {
        selectedText.textContent = `Select ${formatColumnName(column)}`;
    } else if (selectedValues.length === 1) {
        selectedText.textContent = selectedValues[0];
    } else {
        selectedText.textContent = `${selectedValues.length} selected`;
    }
}

// Update CSS styles for dropdowns
const style = document.createElement('style');
style.textContent = `
    .filter-container {
        position: relative;
        z-index: 1500;
        background: #fff;
        padding: 15px;
        margin-bottom: 20px;
        border-bottom: 1px solid #dee2e6;
        width: 100%;
    }

    .table-container {
        position: relative;
        z-index: 1;
    }

    .dropdown-check-list {
        position: relative;
    }

    .dropdown-menu {
        width: 100%;
        position: absolute !important;
        z-index: 2000 !important;
        max-height: 300px;
        overflow-y: auto;
        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
        border: 1px solid rgba(0,0,0,0.15);
        background-color: #fff;
        margin-top: 2px !important;
        padding: 8px;
    }

    /* Ensure dropdowns appear within the filter container */
    .filter-container .dropdown-menu {
        position: absolute !important;
        top: 100% !important;
        left: 0 !important;
        transform: none !important;
        max-width: 100% !important;
    }

    .table-responsive {
        position: relative;
        z-index: 1;
        margin-top: 20px;
    }

    /* Hide table when dropdowns are open */
    .dropdown-open .table-responsive {
        visibility: visible;
    }

    .select2-container {
        z-index: 2000 !important;
    }

    .select2-dropdown {
        z-index: 2001 !important;
        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
        border: 1px solid rgba(0,0,0,0.15);
    }

    /* Rest of the existing styles */
    .options-container {
        max-height: 200px;
        overflow-y: auto;
        margin-top: 8px;
    }

    .form-check {
        padding: 8px;
        margin: 0;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        border-radius: 4px;
    }

    .form-check:hover {
        background-color: rgba(0,0,0,0.05);
    }

    /* Mobile specific styles */
    @media (max-width: 768px) {
        .filter-container {
            position: sticky;
            top: 0;
            background: #fff;
            padding: 10px;
            z-index: 1500;
        }

        .dropdown-menu {
            position: fixed !important;
            top: 50% !important;
            left: 50% !important;
            transform: translate(-50%, -50%) !important;
            width: 90% !important;
            max-width: 400px;
            max-height: 80vh;
        }
    }
`;
document.head.appendChild(style);

// Add new function for client-side filtering
function applyFilters() {
    console.time('applyFilters');
    let filtered = [...state.allData];
    
    // Apply each filter
    Object.entries(state.filters).forEach(([column, value]) => {
        if (!value || (Array.isArray(value) && value.length === 0)) return;
        
        if (column === 'code') {
            filtered = filtered.filter(item => item.code === value);
        } else if (Array.isArray(value)) {
            filtered = filtered.filter(item => value.includes(String(item[column])));
        }
    });
    
    // Update filtered data and pagination
    state.filteredData = filtered;
    state.totalPages = Math.ceil(filtered.length / state.perPage);
    state.currentPage = Math.min(state.currentPage, state.totalPages);
    
    // Get current page data
    const start = (state.currentPage - 1) * state.perPage;
    const end = start + state.perPage;
    state.currentData = filtered.slice(start, end);
    
    console.timeEnd('applyFilters');
}

// Update Select2 specific styles
const select2Styles = document.createElement('style');
select2Styles.textContent = `
    .select2-container--bootstrap-5 .select2-selection {
        min-height: 38px;
        border: 1px solid #ced4da;
    }
    
    .select2-container--bootstrap-5 .select2-selection--multiple {
        padding: 2px 8px;
    }
    
    .select2-container--bootstrap-5 .select2-selection--multiple .select2-selection__choice {
        background-color: #673ab7;
        color: #ffffff;
        border: none;
        padding: 2px 8px;
        margin: 2px 4px;
        border-radius: 4px;
        font-weight: 400;
        display: flex;
        align-items: center;
        gap: 6px;
    }
    
    .select2-container--bootstrap-5 .select2-selection--multiple .select2-selection__choice__remove {
        color: #ffffff !important;
        font-size: 18px;
        order: 1;
        padding: 0 4px;
        border: none;
        background: transparent;
        opacity: 1;
        line-height: 1;
        display: flex;
        align-items: center;
        justify-content: center;
        margin: 0;
    }

    .select2-container--bootstrap-5 .select2-selection--multiple .select2-selection__choice__remove:hover {
        background-color: transparent;
        color: #e0e0e0 !important;
        opacity: 0.9;
    }

    .select2-container--bootstrap-5 .select2-selection--multiple .select2-selection__choice__display {
        color: #ffffff;
        padding: 0;
        order: 0;
        margin: 0;
    }

    /* Override any default Select2 remove button styles */
    .select2-selection__choice__remove span,
    .select2-selection__choice__remove::before,
    .select2-selection__choice__remove::after {
        color: #ffffff !important;
        font-size: 18px !important;
        font-weight: normal !important;
    }

    /* Ensure hover states maintain visibility */
    .select2-selection__choice__remove:hover span,
    .select2-selection__choice__remove:hover::before,
    .select2-selection__choice__remove:hover::after {
        color: #e0e0e0 !important;
    }

    .select2-container--bootstrap-5 .select2-search__field {
        margin-top: 0;
        min-height: 30px;
    }
    
    .select2-container--bootstrap-5 .select2-dropdown {
        border-color: #ced4da;
        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
    }
    
    .select2-container--bootstrap-5 .select2-results__option--highlighted[aria-selected] {
        background-color: #673ab7;
        color: #ffffff;
    }
    
    .select2-container--bootstrap-5 .select2-results__option[aria-selected=true] {
        background-color: #e9ecef;
    }

    .select2-container--bootstrap-5.select2-container--focus .select2-selection {
        border-color: #673ab7;
        box-shadow: 0 0 0 0.2rem rgba(103, 58, 183, 0.25);
    }

    .select2-container--bootstrap-5 .select2-selection--multiple .select2-selection__rendered {
        display: flex;
        flex-wrap: wrap;
        align-items: center;
    }

    .select2-container--bootstrap-5 .select2-selection--multiple .select2-search__field {
        margin-left: 4px;
    }
`;
document.head.appendChild(select2Styles);

// Update table styles for column separators
const tableStyles = document.createElement('style');
tableStyles.textContent = `
    .table {
        border-collapse: separate;
        border-spacing: 0;
        border: 1px solid #000;
    }
    
    .table th,
    .table td {
        border-right: 1px solid #000;
        border-bottom: 1px solid #000;
        padding: 8px;
    }
    
    .table th:last-child,
    .table td:last-child {
        border-right: 1px solid #000;
    }
    
    .table thead th {
        border-bottom: 2px solid #000;
        background-color: #673ab7;
        color: white;
        font-weight: 500;
        vertical-align: middle;
    }

    .table thead {
        border-bottom: 2px solid #000;
    }
    
    .table tbody tr:last-child td {
        border-bottom: 1px solid #000;
    }
    
    .table tbody tr:nth-of-type(odd) {
        background-color: rgba(0, 0, 0, 0.05);
    }
    
    .table tbody tr:hover {
        background-color: rgba(0, 0, 0, 0.075);
    }

    .summary-row {
        background-color: #f8f9fa !important;
        font-weight: 500;
        border-bottom: 2px solid #000;
    }

    .summary-row td {
        text-align: center !important;
        font-size: 0.9em;
        color: #673ab7;
        padding: 4px 8px !important;
    }

    .summary-label {
        font-size: 0.8em;
        display: block;
        color: #666;
        margin-top: 2px;
    }

    .btn-sort {
        background: transparent;
        border: none;
        color: white;
        padding: 0;
        margin-left: 8px;
    }

    .btn-sort:hover {
        color: rgba(255, 255, 255, 0.8);
    }

    .sort-asc svg path:first-child,
    .sort-desc svg path:last-child {
        fill: white;
    }

    .table-responsive {
        border: 1px solid #000;
        border-radius: 4px;
        overflow: hidden;
    }
`;
document.head.appendChild(tableStyles);

// Fetch and populate the Region dropdown on page load
async function populateRegionDropdown() {
    try {
        const response = await fetch('/api/filters/regions');
        if (!response.ok) throw new Error('Failed to fetch regions');
        const regions = await response.json();

        const $region = $('#regionFilter');
        $region.empty().append('<option></option>');
        regions.forEach(region => {
            $region.append(`<option value="${region}">${region}</option>`);
        });
        $region.prop('disabled', false).trigger('change');
    } catch (error) {
        showError('Could not load regions');
    }
}

// Fetch and populate the City dropdown based on selected regions
async function populateCityDropdown(selectedRegions) {
    try {
        if (!selectedRegions || selectedRegions.length === 0) {
            $('#cityFilter').empty().append('<option></option>').prop('disabled', true).trigger('change');
            return;
        }
        const params = new URLSearchParams();
        selectedRegions.forEach(region => params.append('regions', region));
        const response = await fetch(`/api/filters/cities?${params.toString()}`);
        if (!response.ok) throw new Error('Failed to fetch cities');
        const cities = await response.json();
        const $city = $('#cityFilter');
        $city.empty().append('<option></option>');
        cities.forEach(city => {
            $city.append(`<option value="${city}">${city}</option>`);
        });
        $city.prop('disabled', false).trigger('change');
    } catch (error) {
        showError('Could not load cities');
    }
}

// Fetch and populate the Code dropdown based on selected regions and cities
async function populateCodeDropdown(selectedRegions, selectedCities) {
    try {
        if (!selectedCities || selectedCities.length === 0) {
            $('#codeFilter').empty().append('<option></option>').prop('disabled', true).trigger('change');
            return;
        }
        const params = new URLSearchParams();
        selectedRegions.forEach(region => params.append('regions', region));
        selectedCities.forEach(city => params.append('cities', city));
        const response = await fetch(`/api/filters/codes?${params.toString()}`);
        if (!response.ok) throw new Error('Failed to fetch codes');
        const codes = await response.json();
        const $code = $('#codeFilter');
        $code.empty().append('<option></option>');
        codes.forEach(item => {
            const label = item.description ? `${item.code} - ${item.description}` : item.code;
            $code.append(`<option value="${item.code}">${label}</option>`);
        });
        $code.prop('disabled', false).trigger('change');
    } catch (error) {
        showError('Could not load codes');
    }
}

// Fetch and populate the Hospital Name dropdown based on selected regions, cities, and code
async function populateHospitalDropdown(selectedRegions, selectedCities, selectedCode) {
    try {
        if (!selectedCode) {
            $('#hospital_nameFilter').empty().append('<option></option>').prop('disabled', true).trigger('change');
            return;
        }
        const params = new URLSearchParams();
        selectedRegions.forEach(region => params.append('regions', region));
        selectedCities.forEach(city => params.append('cities', city));
        if (selectedCode) params.append('code', selectedCode);
        const response = await fetch(`/api/filters/hospitals?${params.toString()}`);
        if (!response.ok) throw new Error('Failed to fetch hospitals');
        const hospitals = await response.json();
        const $hospital = $('#hospital_nameFilter');
        $hospital.empty().append('<option></option>');
        hospitals.forEach(hospital => {
            $hospital.append(`<option value="${hospital}">${hospital}</option>`);
        });
        $hospital.prop('disabled', false).trigger('change');
    } catch (error) {
        showError('Could not load hospitals');
    }
}

// Populate Payer Name dropdown
async function populatePayerDropdown(selectedRegions, selectedCities, selectedCode, selectedHospitals) {
    try {
        if (!selectedHospitals || selectedHospitals.length === 0) {
            $('#payer_nameFilter').empty().append('<option></option>').prop('disabled', true).trigger('change');
            return;
        }
        const params = new URLSearchParams();
        selectedRegions.forEach(region => params.append('regions', region));
        selectedCities.forEach(city => params.append('cities', city));
        if (selectedCode) params.append('code', selectedCode);
        selectedHospitals.forEach(hospital => params.append('hospital_name', hospital));
        const response = await fetch(`/api/filters/payers?${params.toString()}`);
        if (!response.ok) throw new Error('Failed to fetch payers');
        const payers = await response.json();
        const $payer = $('#payer_nameFilter');
        $payer.empty().append('<option></option>');
        payers.forEach(payer => {
            $payer.append(`<option value="${payer.name}">${payer.name} (${payer.count})</option>`);
        });
        $payer.prop('disabled', false).trigger('change');
    } catch (error) {
        showError('Could not load payers');
    }
}

// Populate Plan Name dropdown
async function populatePlanDropdown(selectedRegions, selectedCities, selectedCode, selectedHospitals, selectedPayers) {
    try {
        if (!selectedPayers || selectedPayers.length === 0) {
            $('#plan_nameFilter').empty().append('<option></option>').prop('disabled', true).trigger('change');
            return;
        }
        const params = new URLSearchParams();
        selectedRegions.forEach(region => params.append('regions', region));
        selectedCities.forEach(city => params.append('cities', city));
        if (selectedCode) params.append('code', selectedCode);
        selectedHospitals.forEach(hospital => params.append('hospital_name', hospital));
        selectedPayers.forEach(payer => params.append('payer_name', payer));
        const response = await fetch(`/api/filters/plans?${params.toString()}`);
        if (!response.ok) throw new Error('Failed to fetch plans');
        const plans = await response.json();
        const $plan = $('#plan_nameFilter');
        $plan.empty().append('<option></option>');
        plans.forEach(plan => {
            $plan.append(`<option value="${plan.name}">${plan.name} (${plan.count})</option>`);
        });
        $plan.prop('disabled', false).trigger('change');
    } catch (error) {
        showError('Could not load plans');
    }
}

// Update fetchReport to accept all filters
async function fetchReport(regions, cities, code, hospitals, payers, plans) {
    try {
        showLoading();
        const params = new URLSearchParams();
        regions.forEach(region => params.append('region', region));
        cities.forEach(city => params.append('city', city));
        params.append('code', code);
        if (hospitals && hospitals.length) {
            hospitals.forEach(hospital => params.append('hospital_name', hospital));
        }
        if (payers && payers.length) {
            payers.forEach(payer => params.append('payer_name', payer));
        }
        if (plans && plans.length) {
            plans.forEach(plan => params.append('plan_name', plan));
        }
        const response = await fetch(`/api/report?${params.toString()}`);
        if (!response.ok) throw new Error('Failed to fetch report');
        const result = await response.json();
        updateTable(result.data || []);
    } catch (error) {
        showError('Could not load report');
    } finally {
        hideLoading();
    }
}

// On page load, show all filters
$(document).ready(async function() {
    $('.table-responsive').hide();
    $('#codeFilter').closest('.col-md-3').show();
    $('#hospital_nameFilter').closest('.col-md-3').show();
    $('#payer_nameFilter').closest('.col-md-3').show();
    $('#plan_nameFilter').closest('.col-md-3').show();
    $('#clearFilters').hide();

    $('#cityFilter').closest('.col-md-3').show();
    $('#cityFilter').prop('disabled', true);
    $('#codeFilter').prop('disabled', true);
    $('#hospital_nameFilter').prop('disabled', true);
    $('#payer_nameFilter').prop('disabled', true);
    $('#plan_nameFilter').prop('disabled', true);

    showLoading();
    $('#regionFilter').select2({ theme: 'bootstrap-5', width: '100%', allowClear: true });
    $('#cityFilter').select2({ theme: 'bootstrap-5', width: '100%', allowClear: true });
    $('#codeFilter').select2({ theme: 'bootstrap-5', width: '100%', allowClear: true, multiple: false });
    $('#hospital_nameFilter').select2({ theme: 'bootstrap-5', width: '100%', allowClear: true, multiple: true });
    $('#payer_nameFilter').select2({ theme: 'bootstrap-5', width: '100%', allowClear: true, multiple: true });
    $('#plan_nameFilter').select2({ theme: 'bootstrap-5', width: '100%', allowClear: true, multiple: true });
    await populateRegionDropdown();
    $('#regionFilter').prop('disabled', false);
    hideLoading();

    // Region -> City
    $('#regionFilter').on('change', async function() {
        const selectedRegions = $(this).val() || [];
        await populateCityDropdown(selectedRegions);
        $('#codeFilter').empty().append('<option></option>').prop('disabled', true).trigger('change');
        $('#hospital_nameFilter').empty().append('<option></option>').prop('disabled', true).trigger('change');
        $('#payer_nameFilter').empty().append('<option></option>').prop('disabled', true).trigger('change');
        $('#plan_nameFilter').empty().append('<option></option>').prop('disabled', true).trigger('change');
        $('.table-responsive').hide();
    });
    // City -> Code
    $('#cityFilter').on('change', async function() {
        const selectedRegions = $('#regionFilter').val() || [];
        const selectedCities = $(this).val() || [];
        await populateCodeDropdown(selectedRegions, selectedCities);
        $('#hospital_nameFilter').empty().append('<option></option>').prop('disabled', true).trigger('change');
        $('#payer_nameFilter').empty().append('<option></option>').prop('disabled', true).trigger('change');
        $('#plan_nameFilter').empty().append('<option></option>').prop('disabled', true).trigger('change');
        $('.table-responsive').hide();
    });
    // Code -> Hospital
    $('#codeFilter').on('change', async function() {
        const selectedRegions = $('#regionFilter').val() || [];
        const selectedCities = $('#cityFilter').val() || [];
        const selectedCode = $(this).val();
        await populateHospitalDropdown(selectedRegions, selectedCities, selectedCode);
        $('#payer_nameFilter').empty().append('<option></option>').prop('disabled', true).trigger('change');
        $('#plan_nameFilter').empty().append('<option></option>').prop('disabled', true).trigger('change');
        $('.table-responsive').hide();
    });
    // Hospital -> Payer
    $('#hospital_nameFilter').on('change', async function() {
        const selectedRegions = $('#regionFilter').val() || [];
        const selectedCities = $('#cityFilter').val() || [];
        const selectedCode = $('#codeFilter').val();
        const selectedHospitals = $(this).val() || [];
        await populatePayerDropdown(selectedRegions, selectedCities, selectedCode, selectedHospitals);
        $('#plan_nameFilter').empty().append('<option></option>').prop('disabled', true).trigger('change');
        $('.table-responsive').hide();
    });
    // Payer -> Plan
    $('#payer_nameFilter').on('change', async function() {
        const selectedRegions = $('#regionFilter').val() || [];
        const selectedCities = $('#cityFilter').val() || [];
        const selectedCode = $('#codeFilter').val();
        const selectedHospitals = $('#hospital_nameFilter').val() || [];
        const selectedPayers = $(this).val() || [];
        await populatePlanDropdown(selectedRegions, selectedCities, selectedCode, selectedHospitals, selectedPayers);
        $('.table-responsive').hide();
    });
    // Plan -> Table
    $('#plan_nameFilter').on('change', async function() {
        const selectedRegions = $('#regionFilter').val() || [];
        const selectedCities = $('#cityFilter').val() || [];
        const selectedCode = $('#codeFilter').val();
        const selectedHospitals = $('#hospital_nameFilter').val() || [];
        const selectedPayers = $('#payer_nameFilter').val() || [];
        const selectedPlans = $(this).val() || [];
        if (selectedCode) {
            await fetchReport(selectedRegions, selectedCities, selectedCode, selectedHospitals, selectedPayers, selectedPlans);
        } else {
            $('.table-responsive').hide();
        }
    });
}); 