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
    'hospital_name',
    'hospital_address',
    'standard_charge_gross',
    'standard_charge_max',
    'standard_charge_min',
    'standard_charge_negotiated_dollar'
];

const FILTER_ORDER = ['region', 'city', 'code', 'payer_name', 'plan_name'];
const MANDATORY_FILTERS = ['code'];
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
        payer_name: [],
        plan_name: []
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
        showLoading();
        await setupFilters();
        setupEventListeners();
        hideLoading();
        
        reportTableBody.innerHTML = '<tr><td colspan="100%" class="text-center">Please select a region to view data</td></tr>';
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
        
        // First fetch distinct regions
        console.log('Fetching regions...');
        const regionsResponse = await fetch(`${API_ENDPOINT}/regions`);
        if (!regionsResponse.ok) throw new Error('Failed to fetch regions');
        const regionsData = await regionsResponse.json();
        
        console.log('Raw regions response:', regionsData);
        console.log('Received regions data:', {
            count: regionsData?.length || 0,
            data: regionsData
        });

        // Create filter values object
        const filterValues = {
            region: [],
            city: [],
            code: [],
            payer_name: [],
            plan_name: []
        };

        // Initialize with regions
        if (regionsData && regionsData.length > 0) {
            filterValues.region = regionsData.map(item => String(item)).filter(Boolean).sort();
            console.log('Processed regions:', filterValues.region);
        }

        // Setup UI with regions
        setupDynamicFilters(Object.keys(filterValues));
        populateFilters(filterValues, new Map());

        // Add event listener for region selection
        const regionFilter = document.getElementById('regionFilter');
        if (regionFilter) {
            $(regionFilter).select2({
                theme: 'bootstrap-5',
                width: '100%',
                placeholder: 'Select Region...',
                allowClear: true,
                data: filterValues.region.map(region => ({
                    id: region,
                    text: region
                }))
            });

            $(regionFilter).on('select2:select', async function(e) {
                state.filters.region = e.params.data.id;
                console.log('Region selected, state:', state.filters);
                if (state.filters.region) {
                    try {
                        showLoading();
                        console.log('Region selected:', state.filters.region);
                        
                        // Fetch cities for the selected region
                        const response = await fetch(`${API_ENDPOINT}/cities?region=${encodeURIComponent(state.filters.region)}`);
                        if (!response.ok) {
                            console.error('Failed to fetch cities:', response.status, response.statusText);
                            showError('Failed to fetch cities');
                            return;
                        }
                        
                        const cities = await response.json();
                        console.log('Received cities:', cities);
                        
                        if (!cities || !Array.isArray(cities)) {
                            console.error('Invalid cities data received:', cities);
                            showError('Invalid data received from server');
                            return;
                        }
                        
                        if (cities.length === 0) {
                            console.warn('No cities found for region:', state.filters.region);
                            showError('No cities found for the selected region');
                            return;
                        }
                        
                        // Update state
                        state.filters.city = null;
                        state.filters.code = null;
                        state.filters.payer_name = [];
                        state.filters.plan_name = [];
                        
                        // Update city filter with the cities
                        const cityFilter = document.getElementById('cityFilter');
                        if (cityFilter) {
                            // Clear existing options
                            $(cityFilter).empty();
                            
                            // Add placeholder option
                            const placeholderOption = new Option('Select City...', '', true, true);
                            $(cityFilter).append(placeholderOption);
                            
                            // Add city options
                            cities.forEach(city => {
                                const option = new Option(city, city);
                                $(cityFilter).append(option);
                            });
                            
                            // Destroy and re-initialize Select2 to refresh the UI
                            if ($(cityFilter).hasClass('select2-hidden-accessible')) {
                                $(cityFilter).select2('destroy');
                            }
                            $(cityFilter).select2({
                                theme: 'bootstrap-5',
                                width: '100%',
                                placeholder: 'Select City...',
                                allowClear: true
                            });
                        }
                        
                        // Reset dependent filters
                        resetDependentFilters(['code', 'payer_name', 'plan_name']);
                        
                        // Clear table until city is selected
                        reportTableBody.innerHTML = '<tr><td colspan="100%" class="text-center">Please select a city to view data</td></tr>';
                    } catch (error) {
                        console.error('Error in region selection:', error);
                        showError('Failed to load cities for the selected region');
                    } finally {
                        hideLoading();
                    }
                }
            });

            // Handle clear event
            $(regionFilter).on('select2:clear', function() {
                console.log('Region cleared');
                // Reset all data and filters
                state.allData = [];
                state.filteredData = [];
                state.currentData = [];
                state.filters = {
                    region: null,
                    city: null,
                    code: null,
                    payer_name: [],
                    plan_name: []
                };
                
                // Reset all filter values
                ['city', 'code', 'payer_name', 'plan_name'].forEach(key => {
                    const filter = $(`#${key}Filter`);
                    if (filter.length) {
                        filter.val(null).trigger('change');
                    }
                });
                
                // Update table
                reportTableBody.innerHTML = '<tr><td colspan="100%" class="text-center">Please select a region to view data</td></tr>';
            });
        }

        // Add event listener for city selection
        const cityFilter = document.getElementById('cityFilter');
        if (cityFilter) {
            $(cityFilter).on('select2:select', async function(e) {
                state.filters.city = e.params.data.id;
                console.log('City selected, state:', state.filters);
                // Only fetch codes if both region and city are selected
                const codeFilter = document.getElementById('codeFilter');
                if (state.filters.region && state.filters.city) {
                    const params = new URLSearchParams({
                        region: state.filters.region,
                        city: state.filters.city
                    });
                    try {
                        showLoading();
                        const response = await fetch(`/api/report/codes?${params}`);
                        const data = await response.json();
                        // Extract unique codes with descriptions
                        const uniqueCodes = [...new Set(data.data.map(item => ({
                            code: item.code,
                            description: item.description || ''
                        })))].filter(item => item.code).sort((a, b) => a.code.localeCompare(b.code));
                        // Update code filter with codes and descriptions
                        if (codeFilter) {
                            $(codeFilter).empty();
                            const placeholderOption = new Option('Select Code...', '', true, true);
                            $(codeFilter).append(placeholderOption);
                            uniqueCodes.forEach(item => {
                                const optionText = item.description ? 
                                    `${item.code} - ${item.description}` : 
                                    item.code;
                                const option = new Option(optionText, item.code);
                                $(codeFilter).append(option);
                            });
                            if (!$(codeFilter).hasClass('select2-hidden-accessible')) {
                                $(codeFilter).select2({
                                    theme: 'bootstrap-5',
                                    width: '100%',
                                    placeholder: 'Select Code...',
                                    allowClear: true
                                });
                            } else {
                                $(codeFilter).trigger('change');
                            }
                            $(codeFilter).select2('open');
                        }
                    } catch (error) {
                        showError('Failed to load codes for the selected city');
                    } finally {
                        hideLoading();
                    }
                } else {
                    // If either region or city is missing, clear the code dropdown
                    if (codeFilter) {
                        $(codeFilter).empty();
                        const placeholderOption = new Option('Select Code...', '', true, true);
                        $(codeFilter).append(placeholderOption);
                        if ($(codeFilter).hasClass('select2-hidden-accessible')) {
                            $(codeFilter).select2('destroy');
                        }
                        $(codeFilter).select2({
                            theme: 'bootstrap-5',
                            width: '100%',
                            placeholder: 'Select Code...',
                            allowClear: true
                        });
                    }
                }
            });

            // Handle clear event
            $(cityFilter).on('select2:clear', function() {
                console.log('City cleared');
                // Reset data and dependent filters
                state.allData = [];
                state.filteredData = [];
                state.currentData = [];
                state.filters.city = null;
                state.filters.code = null;
                state.filters.payer_name = [];
                state.filters.plan_name = [];
                
                // Reset dependent filter values
                ['code', 'payer_name', 'plan_name'].forEach(key => {
                    const filter = $(`#${key}Filter`);
                    if (filter.length) {
                        filter.val(null).trigger('change');
                    }
                });
                
                // Update table
                reportTableBody.innerHTML = '<tr><td colspan="100%" class="text-center">Please select a city to view data</td></tr>';
            });
        }

        // Add event listener for code selection
        const codeFilter = document.getElementById('codeFilter');
        if (codeFilter) {
            $(codeFilter).on('select2:select', async function(e) {
                state.filters.code = e.params.data.id;
                console.log('Code selected, state:', state.filters);
                if (state.filters.region && state.filters.city && state.filters.code) {
                    const params = new URLSearchParams({
                        region: state.filters.region,
                        city: state.filters.city,
                        code: state.filters.code
                    });
                    console.log('Making /api/report API call with params:', params.toString());
                    showLoading();
                    const response = await fetch(`/api/report?${params}`);
                    const data = await response.json();
                    state.allData = data.data || [];
                    state.filteredData = state.allData;
                    state.currentData = state.allData;
                    hideLoading();
                    updateTable();
                }
            });

            // Handle clear event
            $(codeFilter).on('select2:clear', function() {
                console.log('Code cleared');
                // Reset data and dependent filters
                state.allData = [];
                state.filteredData = [];
                state.currentData = [];
                state.filters.code = null;
                state.filters.payer_name = [];
                state.filters.plan_name = [];
                
                // Reset dependent filter values
                ['payer_name', 'plan_name'].forEach(key => {
                    const filter = $(`#${key}Filter`);
                    if (filter.length) {
                        filter.val(null).trigger('change');
                    }
                });
                
                // Update table
                reportTableBody.innerHTML = '<tr><td colspan="100%" class="text-center">Please select a code to view data</td></tr>';
            });
        }

        // Add event listener for payer name selection
        const payerNameFilter = document.getElementById('payer_nameFilter');
        if (payerNameFilter) {
            $(payerNameFilter).on('select2:select select2:unselect', async function(e) {
                const selectedPayerNames = $(this).val() || [];
                state.filters.payer_name = selectedPayerNames;
                // Only fetch if a payer is selected
                if (selectedPayerNames.length > 0) {
                    // Only support one payer at a time for now
                    const params = new URLSearchParams({
                        region: state.filters.region,
                        city: state.filters.city,
                        code: state.filters.code,
                        payer_name: selectedPayerNames[0]
                    });
                    try {
                        showLoading();
                        const response = await fetch(`/api/report?${params}`);
                        const data = await response.json();
                        state.allData = data.data || [];
                        state.filteredData = state.allData;
                        state.currentData = state.allData;
                        updateTable();
                    } catch (error) {
                        showError('Failed to fetch data for selected payer.');
                    } finally {
                        hideLoading();
                    }
                } else {
                    // If no payer selected, show all data for code/region/city
                    state.filteredData = state.allData;
                    state.currentData = state.allData;
                    updateTable();
                }
            });
        }

        // Add event listener for plan name selection
        const planNameFilter = document.getElementById('plan_nameFilter');
        if (planNameFilter) {
            $(planNameFilter).on('select2:select select2:unselect', async function(e) {
                const selectedPlanNames = $(this).val() || [];
                state.filters.plan_name = selectedPlanNames;
                // Only fetch if a plan is selected
                if (selectedPlanNames.length > 0) {
                    // Only support one plan at a time for now
                    const params = new URLSearchParams({
                        region: state.filters.region,
                        city: state.filters.city,
                        code: state.filters.code,
                        payer_name: state.filters.payer_name[0] || '',
                        plan_name: selectedPlanNames[0]
                    });
                    try {
                        showLoading();
                        const response = await fetch(`/api/report?${params}`);
                        const data = await response.json();
                        state.allData = data.data || [];
                        state.filteredData = state.allData;
                        state.currentData = state.allData;
                        updateTable();
                    } catch (error) {
                        showError('Failed to fetch data for selected plan.');
                    } finally {
                        hideLoading();
                    }
                } else {
                    // If no plan selected, show all data for code/region/city/payer
                    state.filteredData = state.allData;
                    state.currentData = state.allData;
                    updateTable();
                }
            });
        }
    } catch (error) {
        console.error('Error in setupFilters:', error);
        hideLoading();
        showError('Failed to load regions');
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
                const response = await fetch(`${API_ENDPOINT}/report/codes?code=${encodeURIComponent(code)}`);
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
            
            const response = await fetch(`${API_ENDPOINT}/report/codes?${params}`);
            
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

// Create a filter element
function createFilterElement(column, isMandatory) {
    console.log(`Creating filter element for ${column}`);
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

    filterContainer.appendChild(filterCol);
    console.log(`Filter element created for ${column}:`, select);
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
    
    console.timeEnd('updateTable');
    console.log('Table update completed');
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