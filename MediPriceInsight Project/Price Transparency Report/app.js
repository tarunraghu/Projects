// Configuration
const API_ENDPOINT = '/api';
const FILTERS_ENDPOINT = '/api/filters';

// DOM Elements
const filterContainer = document.getElementById('filterContainer');
console.log('Filter container:', filterContainer);
if (!filterContainer) {
    console.error('Filter container not found!');
} else {
    console.log('Filter container HTML:', filterContainer.outerHTML);
    console.log('Filter container children:', Array.from(filterContainer.children).map(child => child.outerHTML));
}

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
    'hospital_address',
    'standard_charge_gross',
    'standard_charge_max',
    'standard_charge_min',
    'standard_charge_negotiated_dollar'
];

const FILTER_ORDER = ['region', 'city', 'code', 'payer_name', 'plan_name', 'hospital_name'];
const MANDATORY_FILTERS = ['region', 'city', 'code'];
const MULTI_SELECT_FILTERS = ['city', 'payer_name', 'plan_name', 'hospital_name'];
const DEBOUNCE_DELAY = 300;

// State management
const state = {
    currentData: [],
    filteredData: [],
    allData: [],
    filters: {
        region: null,
        city: null,
        code: null,
        payer_name: null,
        plan_name: null,
        hospital_name: null
    },
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
        console.log('Initializing page...');
        showLoading();
        
        // Setup filter elements
        setupDynamicFilters(FILTER_ORDER);
        
        // Initialize Select2 for all filters
        FILTER_ORDER.forEach(column => {
            const filter = document.getElementById(`${column}Filter`);
            if (filter) {
                console.log(`Initializing ${column} filter...`);
                initializeSelect2(filter, `Select ${formatColumnName(column)}...`);
            }
        });
        
        // Setup event listeners
        setupFilterEventListeners();
        
        // Fetch initial regions
        console.log('Fetching regions...');
        const response = await fetch('/api/regions');
        if (!response.ok) throw new Error('Failed to fetch regions');
        const regions = await response.json();
        console.log('Fetched regions:', regions);
        
        // Update region filter
        const regionFilter = document.getElementById('regionFilter');
        if (regionFilter) {
            console.log('Updating region filter options...');
            $(regionFilter).empty();
            const placeholderOption = new Option('Select Region...', '', true, true);
            $(regionFilter).append(placeholderOption);
            
            regions.forEach(region => {
                const option = new Option(region, region);
                $(regionFilter).append(option);
            });
            
            if ($(regionFilter).hasClass('select2-hidden-accessible')) {
                $(regionFilter).select2('destroy');
            }
            initializeSelect2(regionFilter, 'Select Region...');
            
            // Add direct event listener for region selection
            $(regionFilter).on('select2:select', async function(e) {
                console.log('Region selected:', e.params.data);
                state.filters.region = e.params.data.id;
                console.log('Updated state.filters.region:', state.filters.region);
                await handleFilterChange('region');
            });
        }
        
        hideLoading();
    } catch (error) {
        console.error('Error during initialization:', error);
        showError('Failed to initialize the application');
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

// Create a filter element
function createFilterElement(column, isMandatory) {
    console.log(`Creating filter element for ${column}`);
    const filterCol = document.createElement('div');
    filterCol.className = 'col-md-4 mb-3 filter-row';
    
    const label = document.createElement('label');
    label.className = 'form-label';
    label.textContent = formatColumnName(column) + (isMandatory ? ' *' : '');
    label.htmlFor = `${column}Filter`;

    const select = document.createElement('select');
    select.className = 'form-select';
    select.id = `${column}Filter`;
    
    filterCol.appendChild(label);
    filterCol.appendChild(select);

    filterContainer.appendChild(filterCol);
    console.log(`Filter element created for ${column}:`, select);
}

// Initialize Select2 for a filter
function initializeSelect2(filter, placeholder) {
    console.log(`Initializing Select2 for ${filter.id} with placeholder: ${placeholder}`);
    const isMultiSelect = MULTI_SELECT_FILTERS.includes(filter.id.replace('Filter', ''));
    
    $(filter).select2({
        theme: 'bootstrap-5',
        width: '100%',
        placeholder: placeholder,
        allowClear: true,
        multiple: isMultiSelect,
        closeOnSelect: !isMultiSelect,
        templateResult: function(data) {
            if (!data.id) return data.text;
            return $('<span>').text(data.text);
        },
        templateSelection: function(data) {
            if (!data.id) return data.text;
            return $('<span class="selected-chip">').text(data.text);
        }
    });

    // Add custom styles for chips
    const style = document.createElement('style');
    style.textContent = `
        .selected-chip {
            background-color: #673ab7;
            color: white;
            padding: 2px 8px;
            border-radius: 4px;
            margin: 2px;
            display: inline-block;
        }
        
        .select2-container--bootstrap-5 .select2-selection--multiple .select2-selection__choice {
            background-color: #673ab7;
            color: white;
            border: none;
            padding: 2px 8px;
            margin: 2px;
            border-radius: 4px;
        }
        
        .select2-container--bootstrap-5 .select2-selection--multiple .select2-selection__choice__remove {
            color: white;
            margin-left: 8px;
            border: none;
            background: transparent;
            padding: 0 4px;
        }
        
        .select2-container--bootstrap-5 .select2-selection--multiple .select2-selection__choice__remove:hover {
            background-color: rgba(255, 255, 255, 0.2);
            color: white;
        }
    `;
    document.head.appendChild(style);
}

// Setup event listeners for filters
function setupFilterEventListeners() {
    FILTER_ORDER.forEach(column => {
        const filter = document.getElementById(`${column}Filter`);
        if (filter) {
            // Remove any existing event listeners
            $(filter).off('select2:select select2:clear');
            
            // Add new event listeners
            $(filter).on('select2:select', async function(e) {
                state.filters[column] = e.params.data.id;
                console.log(`${column} selected:`, state.filters[column]);
                await handleFilterChange(column);
            });

            $(filter).on('select2:clear', async function() {
                state.filters[column] = null;
                console.log(`${column} cleared`);
                await handleFilterChange(column);
            });
        }
    });
}

// Handle filter changes
async function handleFilterChange(changedFilter) {
    try {
        showLoading();
        console.log('Filter change:', changedFilter, 'Current state:', state.filters);
        
        const { region, city, code, payer_name, plan_name, hospital_name } = state.filters;
        
        // Convert arrays to comma-separated strings for API calls
        const cityParam = Array.isArray(city) ? city.join(',') : city;
        const payerNameParam = Array.isArray(payer_name) ? payer_name.join(',') : payer_name;
        const planNameParam = Array.isArray(plan_name) ? plan_name.join(',') : plan_name;
        const hospitalNameParam = Array.isArray(hospital_name) ? hospital_name.join(',') : hospital_name;
        
        switch (changedFilter) {
            case 'region':
                if (region) {
                    const response = await fetch(`/api/cities?region=${encodeURIComponent(region)}`);
                    if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
                    const cities = await response.json();
                    updateCityFilter(cities);
                    state.filters.city = [];
                    state.filters.code = null;
                    state.filters.payer_name = [];
                    state.filters.plan_name = [];
                    state.filters.hospital_name = [];
                }
                break;
                
            case 'city':
                if (region && city) {
                    const response = await fetch(`/api/report/codes?region=${encodeURIComponent(region)}&city=${encodeURIComponent(cityParam)}`);
                    if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
                    const data = await response.json();
                    updateCodeFilter(data.data || []);
                    state.filters.code = null;
                    state.filters.payer_name = [];
                    state.filters.plan_name = [];
                    state.filters.hospital_name = [];
                }
                break;
                
            case 'code':
                if (region && city && code) {
                    const url = `/api/report?region=${encodeURIComponent(region)}&city=${encodeURIComponent(cityParam)}&code=${encodeURIComponent(code)}`;
                    console.log('Fetching data from:', url);
                    
                    const response = await fetch(url);
                    if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
                    const data = await response.json();
                    
                    state.allData = data.data || [];
                    state.filteredData = state.allData;
                    state.currentData = state.allData;
                    
                    // Update optional filters with current data
                    const uniquePayers = [...new Set(state.allData.map(item => item.payer_name).filter(Boolean))].sort();
                    const uniquePlans = [...new Set(state.allData.map(item => item.plan_name).filter(Boolean))].sort();
                    const uniqueHospitals = [...new Set(state.allData.map(item => item.hospital_name).filter(Boolean))].sort();
                    
                    updateOptionalFilter('payer_name', uniquePayers);
                    updateOptionalFilter('plan_name', uniquePlans);
                    updateOptionalFilter('hospital_name', uniqueHospitals);
                    
                    state.filters.payer_name = [];
                    state.filters.plan_name = [];
                    state.filters.hospital_name = [];
                    
                    updateTable();
                }
                break;
                
            case 'payer_name':
            case 'plan_name':
            case 'hospital_name':
                if (region && city && code) {
                    console.log(`Filtering by ${changedFilter}:`, state.filters[changedFilter]);
                    let filteredData = state.allData;
                    
                    // Apply all optional filters
                    if (payer_name && payer_name.length > 0) {
                        filteredData = filteredData.filter(item => payer_name.includes(item.payer_name));
                    }
                    if (plan_name && plan_name.length > 0) {
                        filteredData = filteredData.filter(item => plan_name.includes(item.plan_name));
                    }
                    if (hospital_name && hospital_name.length > 0) {
                        filteredData = filteredData.filter(item => hospital_name.includes(item.hospital_name));
                    }
                    
                    state.filteredData = filteredData;
                    state.currentData = filteredData;
                    updateTable();
                }
                break;
        }
    } catch (error) {
        console.error('Error in handleFilterChange:', error);
        showError('Failed to update data: ' + error.message);
    } finally {
        hideLoading();
    }
}

// Update city filter options
function updateCityFilter(cities) {
    const cityFilter = document.getElementById('cityFilter');
    if (cityFilter) {
        $(cityFilter).empty();
        const placeholderOption = new Option('Select City...', '', true, true);
        $(cityFilter).append(placeholderOption);
        
        cities.forEach(city => {
            const option = new Option(city, city);
            $(cityFilter).append(option);
        });
        
        if ($(cityFilter).hasClass('select2-hidden-accessible')) {
            $(cityFilter).select2('destroy');
        }
        initializeSelect2(cityFilter, 'Select City...');
    }
}

// Update code filter options
function updateCodeFilter(codes) {
    const codeFilter = document.getElementById('codeFilter');
    if (codeFilter) {
        $(codeFilter).empty();
        const placeholderOption = new Option('Select Code...', '', true, true);
        $(codeFilter).append(placeholderOption);
        
        codes.forEach(item => {
            const optionText = item.description ? 
                `${item.code} - ${item.description}` : 
                item.code;
            const option = new Option(optionText, item.code);
            $(codeFilter).append(option);
        });
        
        if ($(codeFilter).hasClass('select2-hidden-accessible')) {
            $(codeFilter).select2('destroy');
        }
        initializeSelect2(codeFilter, 'Select Code...');
    }
}

// Update optional filter options
function updateOptionalFilter(filterName, values) {
    console.log(`Updating ${filterName} filter with:`, values);
    const filter = document.getElementById(`${filterName}Filter`);
    if (!filter) {
        console.error(`${filterName} filter element not found`);
        return;
    }

    // First, destroy any existing Select2 instance
    if ($(filter).hasClass('select2-hidden-accessible')) {
        $(filter).select2('destroy');
    }

    // Clear existing options
    $(filter).empty();

    // Add placeholder option
    const placeholderOption = new Option(`Select ${formatColumnName(filterName)}...`, '', true, true);
    $(filter).append(placeholderOption);

    // Add options
    values.forEach(value => {
        if (value) {  // Only add non-null values
            const option = new Option(value, value);
            $(filter).append(option);
        }
    });

    // Initialize Select2 with proper configuration
    const isMultiSelect = MULTI_SELECT_FILTERS.includes(filterName);
    $(filter).select2({
        theme: 'bootstrap-5',
        width: '100%',
        placeholder: `Select ${formatColumnName(filterName)}...`,
        allowClear: true,
        multiple: isMultiSelect,
        closeOnSelect: !isMultiSelect,
        templateResult: function(data) {
            if (!data.id) return data.text;
            return $('<span>').text(data.text);
        },
        templateSelection: function(data) {
            if (!data.id) return data.text;
            return $('<span class="selected-chip">').text(data.text);
        }
    });

    // Add event listeners
    $(filter).off('select2:select select2:clear select2:unselect').on({
        'select2:select': function(e) {
            console.log(`${filterName} selected:`, e.params.data.id);
            if (isMultiSelect) {
                const currentValues = state.filters[filterName] || [];
                state.filters[filterName] = [...currentValues, e.params.data.id];
            } else {
                state.filters[filterName] = e.params.data.id;
            }
            handleFilterChange(filterName);
        },
        'select2:unselect': function(e) {
            if (isMultiSelect) {
                const currentValues = state.filters[filterName] || [];
                state.filters[filterName] = currentValues.filter(v => v !== e.params.data.id);
                handleFilterChange(filterName);
            }
        },
        'select2:clear': function() {
            console.log(`${filterName} cleared`);
            state.filters[filterName] = isMultiSelect ? [] : null;
            handleFilterChange(filterName);
        }
    });
}

// Setup dynamic filters
function setupDynamicFilters(columns) {
    console.log('Setting up dynamic filters for columns:', columns);
    filterContainer.innerHTML = '';

    // Create filters in specified order
    FILTER_ORDER.forEach(column => {
        if (columns.includes(column)) {
            console.log(`Creating filter for ${column}...`);
            createFilterElement(column, MANDATORY_FILTERS.includes(column));
        }
    });
}

// Populate filters with values
function populateFilters(filterValues, uniqueCodes) {
    console.log('Populating filters with values:', filterValues);
    state.filterOptions = filterValues;
    
    Object.entries(filterValues).forEach(([column, values]) => {
        const filter = document.getElementById(`${column}Filter`);
        if (!filter) {
            console.warn(`Filter element not found for column: ${column}`);
            return;
        }

        if (column === 'region') {
            console.log(`Populating ${values.length} regions...`);
            
            // Format options for regions
            const options = values.map(region => ({
                id: region,
                text: region
            }));

            console.log('Region options:', options);
            
            $(filter).empty().append('<option></option>');
            $(filter).select2({
                theme: 'bootstrap-5',
                width: '100%',
                placeholder: 'Select Region...',
                allowClear: true,
                data: options
            });

            // Add event listener for region selection
            $(filter).on('select2:select', async function(e) {
                state.filters.region = e.params.data.id;
                console.log('Region selected:', state.filters.region);
                await handleFilterChange('region');
            });

            $(filter).on('select2:clear', async function() {
                state.filters.region = null;
                console.log('Region cleared');
                await handleFilterChange('region');
            });
        } else {
            // For other filters, initialize with empty state
            $(filter).empty().append('<option></option>');
            $(filter).select2({
                theme: 'bootstrap-5',
                width: '100%',
                placeholder: `Select ${formatColumnName(column)}...`,
                allowClear: true,
                data: []  // Start with empty data
            });
        }
    });
}

// Populate filter options
function populateFilterOptions(column, values) {
    console.log(`Populating filter options for ${column}:`, values);
    const filter = document.getElementById(`${column}Filter`);
    if (!filter) {
        console.error(`Filter element not found for ${column}`);
        return;
    }

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
        console.log('Processing non-code filter:', column);
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

        console.log('Value counts for', column, ':', valueCounts);

        options = Array.from(valueCounts.entries())
            .map(([value, count]) => ({
                id: value,
                text: `${value} (${count})`,
                count: count
            }))
            .sort((a, b) => a.id.localeCompare(b.id));
        
        console.log('Generated options for', column, ':', options);
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
    
    console.log(`Successfully populated ${column} filter with options`);
    
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
        // Get current filtered data based on region selection
        let filteredData = [...state.allData];
        
        // Filter data based on selected region first
        const selectedRegion = state.filters.region;
        if (selectedRegion) {
            filteredData = filteredData.filter(item => item.region === selectedRegion);
        }
        
        // Special handling for city selection
        if (changedFilter === 'city') {
            const selectedCity = state.filters.city;
            if (selectedCity) {
                // Filter data based on selected city
                filteredData = filteredData.filter(item => item.city === selectedCity);
                
                // Get unique codes for the selected city
                const uniqueCodes = [...new Set(filteredData.map(item => item.code))].filter(Boolean);
                
                // Update code filter with counts
                const codeCounts = uniqueCodes.map(code => ({
                    id: code,
                    count: filteredData.filter(item => item.code === code).length
                }));
                
                // Update code filter options
                updateFilterOptions('code', codeCounts.map(c => c.id));
                
                // Reset dependent filters
                resetDependentFilters(['payer_name', 'plan_name']);
            } else {
                resetDependentFilters(['code', 'payer_name', 'plan_name']);
            }
        }

        // Update payer_name based on region, city, and code selections
        if (changedFilter === 'code' || changedFilter === 'city' || changedFilter === 'region') {
            let currentData = filteredData;
            
            // Apply all previous filters
            const selectedRegion = state.filters.region;
            if (selectedRegion) {
                currentData = currentData.filter(item => item.region === selectedRegion);
            }
            
            const selectedCity = state.filters.city;
            if (selectedCity) {
                currentData = currentData.filter(item => item.city === selectedCity);
            }
            
            const selectedCode = state.filters.code;
            if (selectedCode) {
                currentData = currentData.filter(item => item.code === selectedCode);
            }
            
            // Get unique payer names for the current filters
            const uniquePayers = [...new Set(currentData.map(item => item.payer_name))].filter(Boolean);
            updateFilterOptions('payer_name', uniquePayers);
            
            // Get unique plan names for the current filters
            const uniquePlans = [...new Set(currentData.map(item => item.plan_name))].filter(Boolean);
            updateFilterOptions('plan_name', uniquePlans);
        }
        
        // If code is selected, ensure region and city are maintained
        if (changedFilter === 'code') {
            const selectedCode = state.filters.code;
            if (selectedCode) {
                // Find the region and city for the selected code
                const codeData = state.allData.find(item => item.code === selectedCode);
                if (codeData) {
                    // Update the state with the found region and city
                    state.filters.region = codeData.region;
                    state.filters.city = codeData.city;
                    
                    // Update the UI to reflect these values with chips
                    const regionFilter = document.getElementById('regionFilter');
                    const cityFilter = document.getElementById('cityFilter');
                    ensureOptionAndSetValue($(regionFilter), codeData.region);
                    ensureOptionAndSetValue($(cityFilter), codeData.city);
                }
            }
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
    console.log(`Updating filter options for ${filterName}:`, values);
    const filter = $(`#${filterName}Filter`);
    if (!filter.length) {
        console.error(`Filter element not found for ${filterName}`);
        return;
    }
    
    const currentSelected = filter.val() || [];
    console.log('Current selected values:', currentSelected);
    
    // Only keep currently selected values that are still valid
    const validSelected = currentSelected.filter(value => values.includes(value));
    console.log('Valid selected values:', validSelected);
    
    // Update options and selection
    populateFilterOptions(filterName, values);
    if (validSelected.length > 0) {
        filter.val(validSelected).trigger('change');
    } else {
        filter.val(null).trigger('change');
    }
    
    // Update state
    state.filters[filterName] = validSelected;
    console.log(`Updated ${filterName} filter state:`, state.filters[filterName]);
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
function updateTable() {
    console.time('updateTable');
    console.log('Updating table with data:', {
        currentDataCount: state.currentData.length,
        filteredDataCount: state.filteredData.length
    });
    
    if (!state.currentData.length) {
        reportTableBody.innerHTML = '<tr><td colspan="100%" class="text-center">No data found for the selected filters</td></tr>';
        return;
    }

    // Define the column mapping
    const columnMap = {
        'region': 'Region',
        'city': 'City',
        'code': 'Code',
        'description': 'Description',
        'hospital_name': 'Hospital Name',
        'hospital_address': 'Hospital Address',
        'payer_name': 'Payer Name',
        'plan_name': 'Plan Name',
        'standard_charge_min': 'Standard Charge Min',
        'standard_charge_max': 'Standard Charge Max',
        'standard_charge_gross': 'Standard Charge Gross',
        'standard_charge_negotiated_dollar': 'Standard Charge Negotiated'
    };

    // Calculate unique counts for each column
    const uniqueCounts = {};
    Object.keys(columnMap).forEach(key => {
        const uniqueValues = new Set(state.filteredData.map(item => 
            item[key] !== null && item[key] !== undefined ? String(item[key]) : ''
        ).filter(Boolean));
        uniqueCounts[key] = uniqueValues.size;
    });

    // Setup table headers
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
        headerContent.appendChild(sortButton);
        headerContent.insertBefore(textSpan, headerContent.firstChild);
        th.appendChild(headerContent);
        headerRow.appendChild(th);
    });
    
    tableHeader.innerHTML = '';
    tableHeader.appendChild(headerRow);

    // Add summary row
    const summaryRow = document.createElement('tr');
    summaryRow.className = 'summary-row';
    Object.keys(columnMap).forEach(key => {
        const td = document.createElement('td');
        td.innerHTML = `
            <strong>${uniqueCounts[key]?.toLocaleString() || '0'}</strong>
            <span class="summary-label">unique</span>
        `;
        summaryRow.appendChild(td);
    });
    tableHeader.appendChild(summaryRow);

    // Add data rows
    const fragment = document.createDocumentFragment();
    state.currentData.forEach(item => {
        const row = document.createElement('tr');
        Object.keys(columnMap).forEach(key => {
            const td = document.createElement('td');
            const value = item[key];
            td.textContent = formatValue(value, key);
            if (key !== 'code' && (typeof value === 'number' || (value && !isNaN(value)))) {
                td.className = 'text-end';
            }
            row.appendChild(td);
        });
        fragment.appendChild(row);
    });

    reportTableBody.innerHTML = '';
    reportTableBody.appendChild(fragment);
    
    // Update optional filters with current data
    const uniquePayers = [...new Set(state.currentData.map(item => item.payer_name).filter(Boolean))].sort();
    const uniquePlans = [...new Set(state.currentData.map(item => item.plan_name).filter(Boolean))].sort();
    const uniqueHospitals = [...new Set(state.currentData.map(item => item.hospital_name).filter(Boolean))].sort();
    
    updateOptionalFilter('payer_name', uniquePayers);
    updateOptionalFilter('plan_name', uniquePlans);
    updateOptionalFilter('hospital_name', uniqueHospitals);
    
    console.timeEnd('updateTable');
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
                    const selectedCode = e.type === 'select2:clear' ? '' : e.params?.data?.id || '';
                    state.filters[column] = selectedCode;
                    
                    if (e.type === 'select2:clear') {
                        // Clear everything
                        state.currentData = [];
                        state.filteredData = [];
                        state.allData = [];
                        reportTableBody.innerHTML = '<tr><td colspan="100%" class="text-center">Please select a code to view data</td></tr>';
                        resetDependentFilters(['payer_name', 'plan_name']);
                    } else {
                        // Fetch all data for the selected code
                        await fetchData(1, true);
                        
                        // Update dependent filters
                        await updateDependentFilters('code');
                        
                        // Ensure region and city are maintained in the UI with chips
                        const regionFilter = document.getElementById('regionFilter');
                        const cityFilter = document.getElementById('cityFilter');
                        ensureOptionAndSetValue($(regionFilter), state.filters.region);
                        ensureOptionAndSetValue($(cityFilter), state.filters.city);
                        
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

// Helper to ensure Select2 option exists and set value
function ensureOptionAndSetValue($select, value) {
    if (!$select.length || !value) return;
    if ($select.find(`option[value='${value}']`).length === 0) {
        $select.append(new Option(value, value, true, true));
    }
    $select.val(value).trigger('change');
}

// Helper to get clean city value for API calls
function getCleanCityValue(city) {
    const cityArray = Array.isArray(city) ? city : [city];
    return cityArray.filter(Boolean).join(',');
}

// Add CSS to hide the Select2 chip for the city filter
const hideCityChipStyle = document.createElement('style');
hideCityChipStyle.textContent = `
#cityFilter + .select2-container .select2-selection__choice {
    display: none !important;
}
`;
document.head.appendChild(hideCityChipStyle);

// Add Reset All Filters button above the filters
const resetButton = document.createElement('button');
resetButton.textContent = 'Reset All Filters';
resetButton.className = 'btn btn-secondary mb-3';
resetButton.style.marginRight = '10px';
resetButton.onclick = function() {
    // Reset all filter values in state
    state.filters = {
        region: null,
        city: null,
        code: null,
        payer_name: null,
        plan_name: null,
        hospital_name: null
    };
    // Reset Select2 dropdowns
    ['region', 'city', 'code', 'payer_name', 'plan_name', 'hospital_name'].forEach(key => {
        const filter = document.getElementById(`${key}Filter`);
        if (filter && $(filter).hasClass('select2-hidden-accessible')) {
            $(filter).val(null).trigger('change');
        }
    });
    // Reset data
    state.allData = [];
    state.filteredData = [];
    state.currentData = [];
    // Update table
    reportTableBody.innerHTML = '<tr><td colspan="100%" class="text-center">Please select a region to view data</td></tr>';
    // Optionally, reset pagination
    state.currentPage = 1;
    state.totalPages = 1;
    updatePagination();
};
// Insert the button above the filter container
filterContainer.parentNode.insertBefore(resetButton, filterContainer); 