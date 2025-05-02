// Configuration
const API_ENDPOINT = '/api/report';
const FILTERS_ENDPOINT = '/api/filters';
const STATISTICS_ENDPOINT = '/api/statistics';

// State management
const state = {
    currentData: [],
    filters: {
        code: '',
        region: [],
        city: [],
        payer_name: [],
        plan_name: []
    },
    currentPage: 1,
    perPage: 100,
    totalPages: 1,
    isLoading: false,
    codeDescriptions: {},
    allFilters: {} // Store all available filter options
};

// Initialize the page
$(document).ready(async function() {
    try {
        showLoading();
        await populateRegionDropdown();
        disableFilters(['city', 'code', 'payer_name', 'plan_name']);
        setupEventListeners();
        await fetchStatistics();
        renderActiveFilterChips();
        hideLoading();
    } catch (error) {
        console.error('Error during initialization:', error);
        showError('Failed to initialize the application');
    }
});

// Loading state management
function showLoading() {
    state.isLoading = true;
    document.getElementById('loadingIndicator').classList.remove('d-none');
}

function hideLoading() {
    state.isLoading = false;
    document.getElementById('loadingIndicator').classList.add('d-none');
}

// Error handling
function showError(message) {
    alert(message);
}

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

// Setup filters
async function setupFilters() {
    try {
        const response = await fetch(FILTERS_ENDPOINT);
        if (!response.ok) throw new Error('Failed to fetch filters');
        const data = await response.json();
        
        state.codeDescriptions = data.code_descriptions;
        state.allFilters = data.filters;
        
        // Initialize Select2 for all filters
        $('.select2').select2({
            theme: 'bootstrap-5',
            width: '100%',
            allowClear: true
        });
        
        // Initialize region filter first
        updateRegionFilter();
        
        // Disable all filters except region initially
        disableFilters(['city', 'code', 'payer_name', 'plan_name']);
        
    } catch (error) {
        console.error('Error setting up filters:', error);
        throw error;
    }
}

// Update region filter
function updateRegionFilter() {
    const options = state.allFilters.region.map(value => ({
        id: value,
        text: value
    }));
    
    $('#regionFilter').empty().append('<option></option>');
    $('#regionFilter').select2({
        data: options,
        placeholder: 'Select region...',
        allowClear: true,
        multiple: true
    });
}

// Update city filter based on selected regions
async function updateCityFilter() {
    if (!state.filters.region.length) {
        $('#cityFilter').empty().append('<option></option>').select2({
            placeholder: 'Select city...',
            allowClear: true,
            multiple: true
        });
        disableFilters(['city', 'code', 'payer_name', 'plan_name']);
        return;
    }

    try {
        const response = await fetch(`${FILTERS_ENDPOINT}/cities?regions=${state.filters.region.join(',')}`);
        if (!response.ok) throw new Error('Failed to fetch cities');
        const cities = await response.json();
        
        const options = cities.map(value => ({
            id: value,
            text: value
        }));
        
        $('#cityFilter').empty().append('<option></option>');
        $('#cityFilter').select2({
            data: options,
            placeholder: 'Select city...',
            allowClear: true,
            multiple: true
        });
        
        enableFilter('city');
        disableFilters(['code', 'payer_name', 'plan_name']);
    } catch (error) {
        console.error('Error updating city filter:', error);
        showError('Failed to load cities');
    }
}

// Update code filter based on selected cities
async function updateCodeFilter() {
    if (!state.filters.city.length) {
        $('#codeFilter').empty().append('<option></option>').select2({
            placeholder: 'Select code...',
            allowClear: true,
            multiple: false,
            closeOnSelect: true
        });
        disableFilters(['code', 'payer_name', 'plan_name']);
        return;
    }

    try {
        const response = await fetch(`${FILTERS_ENDPOINT}/codes?cities=${state.filters.city.join(',')}`);
        if (!response.ok) throw new Error('Failed to fetch codes');
        const codes = await response.json();
        
        const options = codes.map(code => ({
            id: code.code || code, // support both {code, description} and string
            text: code.description ? `${code.code} - ${code.description}` : (code.code || code)
        }));
        
        $('#codeFilter').empty().append('<option></option>');
        $('#codeFilter').select2({
            data: options,
            placeholder: 'Search by code or description...',
            allowClear: true,
            multiple: false,
            closeOnSelect: true
        });
        
        enableFilter('code');
        disableFilters(['payer_name', 'plan_name']);
    } catch (error) {
        console.error('Error updating code filter:', error);
        showError('Failed to load codes');
    }
}

// Update payer filter based on selected code
async function updatePayerFilter() {
    if (!state.filters.code) {
        $('#payer_nameFilter').empty().append('<option></option>').select2({
            placeholder: 'Select payer...',
            allowClear: true,
            multiple: true
        });
        disableFilters(['payer_name', 'plan_name']);
        return;
    }

    try {
        const response = await fetch(`${FILTERS_ENDPOINT}/payers?code=${state.filters.code}&cities=${state.filters.city.join(',')}`);
        if (!response.ok) throw new Error('Failed to fetch payers');
        const payers = await response.json();
        
        const options = payers.map(value => ({
            id: value,
            text: value
        }));
        
        $('#payer_nameFilter').empty().append('<option></option>');
        $('#payer_nameFilter').select2({
            data: options,
            placeholder: 'Select payer...',
            allowClear: true,
            multiple: true
        });
        
        enableFilter('payer_name');
        disableFilter('plan_name');
    } catch (error) {
        console.error('Error updating payer filter:', error);
        showError('Failed to load payers');
    }
}

// Update plan filter based on selected payer
async function updatePlanFilter() {
    if (!state.filters.payer_name.length) {
        $('#plan_nameFilter').empty().append('<option></option>').select2({
            placeholder: 'Select plan...',
            allowClear: true,
            multiple: true
        });
        disableFilter('plan_name');
        return;
    }

    try {
        const response = await fetch(`${FILTERS_ENDPOINT}/plans?payers=${state.filters.payer_name.join(',')}&code=${state.filters.code}&cities=${state.filters.city.join(',')}`);
        if (!response.ok) throw new Error('Failed to fetch plans');
        const plans = await response.json();
        
        const options = plans.map(value => ({
            id: value,
            text: value
        }));
        
        $('#plan_nameFilter').empty().append('<option></option>');
        $('#plan_nameFilter').select2({
            data: options,
            placeholder: 'Select plan...',
            allowClear: true,
            multiple: true
        });
        
        enableFilter('plan_name');
    } catch (error) {
        console.error('Error updating plan filter:', error);
        showError('Failed to load plans');
    }
}

// Helper functions for enabling/disabling filters
function disableFilters(filters) {
    filters.forEach(filter => {
        $(`#${filter}Filter`).prop('disabled', true);
        $(`#${filter}Filter`).val(null).trigger('change');
    });
}

function enableFilter(filter) {
    $(`#${filter}Filter`).prop('disabled', false);
}

// Setup event listeners
function setupEventListeners() {
    // Region filter change
    $('#regionFilter').off('select2:select').on('change', async function() {
        state.filters.region = $(this).val() || [];
        state.filters.city = [];
        state.filters.code = '';
        state.filters.payer_name = [];
        state.filters.plan_name = [];
        await updateCityFilter();
        renderActiveFilterChips();
        await fetchData();
    });
    // City filter change
    $('#cityFilter').off('select2:select').on('change', async function() {
        state.filters.city = $(this).val() || [];
        state.filters.code = '';
        state.filters.payer_name = [];
        state.filters.plan_name = [];
        await updateCodeFilter();
        renderActiveFilterChips();
        await fetchData();
    });
    // Code filter change (single select)
    $('#codeFilter').off('select2:select').on('change', async function() {
        state.filters.code = $(this).val() || '';
        state.filters.payer_name = [];
        state.filters.plan_name = [];
        await updatePayerFilter();
        renderActiveFilterChips();
        await fetchData();
    });
    // Payer filter change
    $('#payer_nameFilter').off('select2:select').on('change', async function() {
        state.filters.payer_name = $(this).val() || [];
        state.filters.plan_name = [];
        await updatePlanFilter();
        renderActiveFilterChips();
        await fetchData();
    });
    // Plan filter change
    $('#plan_nameFilter').off('select2:select').on('change', async function() {
        state.filters.plan_name = $(this).val() || [];
        renderActiveFilterChips();
        await fetchData();
    });
    // Clear filters button
    $('#clearFilters').on('click', async function() {
        state.filters = {
            code: '',
            region: [],
            city: [],
            payer_name: [],
            plan_name: []
        };
        $('.select2').val(null).trigger('change');
        disableFilters(['city', 'code', 'payer_name', 'plan_name']);
        renderActiveFilterChips();
        await fetchData();
    });
}

// Fetch data from the backend
async function fetchData(page = 1) {
    if (state.isLoading) return;
    
    try {
        showLoading();
        state.currentPage = page;
        
        const params = new URLSearchParams({
            page: state.currentPage,
            per_page: state.perPage
        });
        
        if (state.filters.code) params.append('code', state.filters.code);
        if (state.filters.region.length) params.append('region', state.filters.region.join(','));
        if (state.filters.city.length) params.append('city', state.filters.city.join(','));
        if (state.filters.payer_name.length) params.append('payer_name', state.filters.payer_name.join(','));
        if (state.filters.plan_name.length) params.append('plan_name', state.filters.plan_name.join(','));
        
        const response = await fetch(`${API_ENDPOINT}?${params}`);
        if (!response.ok) throw new Error('Failed to fetch data');
        
        const result = await response.json();
        state.currentData = result.data;
        state.totalPages = result.total_pages;
        
        updateTable();
        updatePagination();
        updateStatistics();
    } catch (error) {
        console.error('Error fetching data:', error);
        showError('Failed to load data');
    } finally {
        hideLoading();
    }
}

// Fetch statistics
async function fetchStatistics() {
    try {
        const response = await fetch(STATISTICS_ENDPOINT);
        if (!response.ok) throw new Error('Failed to fetch statistics');
        
        const data = await response.json();
        updateStatistics(data);
    } catch (error) {
        console.error('Error fetching statistics:', error);
    }
}

// Update statistics display
function updateStatistics(data) {
    if (data) {
        document.getElementById('totalRecords').textContent = 
            `${data.overall.total_records.toLocaleString()} records`;
        document.getElementById('avgMinCharge').textContent = 
            `$${data.prices.avg_min_charge.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}`;
        document.getElementById('avgMaxCharge').textContent = 
            `$${data.prices.avg_max_charge.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}`;
        document.getElementById('avgNegotiatedCharge').textContent = 
            `$${data.prices.avg_negotiated_charge.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}`;
    }
}

// Update the table with filtered data
function updateTable() {
    const tbody = document.getElementById('reportTableBody');
    tbody.innerHTML = '';
    
    if (!state.currentData.length) {
        tbody.innerHTML = '<tr><td colspan="10" class="text-center">No data found for the selected filters</td></tr>';
        return;
    }
    
    state.currentData.forEach(item => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${item.hospital_name || ''}</td>
            <td>${item.code || ''}</td>
            <td>${item.description || ''}</td>
            <td>${item.city || ''}</td>
            <td>${item.region || ''}</td>
            <td>${item.payer_name || ''}</td>
            <td>${item.plan_name || ''}</td>
            <td class="text-end">$${formatCurrency(item.standard_charge_min)}</td>
            <td class="text-end">$${formatCurrency(item.standard_charge_max)}</td>
            <td class="text-end">$${formatCurrency(item.standard_charge_negotiated_dollar)}</td>
        `;
        tbody.appendChild(row);
    });
    
    // Update showing count
    document.getElementById('showingCount').textContent = state.currentData.length;
    document.getElementById('totalCount').textContent = state.totalPages * state.perPage;
}

// Update pagination controls
function updatePagination() {
    const pagination = document.getElementById('pagination');
    pagination.innerHTML = '';
    
    const maxVisiblePages = 5;
    let startPage = Math.max(1, state.currentPage - Math.floor(maxVisiblePages / 2));
    let endPage = Math.min(state.totalPages, startPage + maxVisiblePages - 1);
    
    if (endPage - startPage + 1 < maxVisiblePages) {
        startPage = Math.max(1, endPage - maxVisiblePages + 1);
    }
    
    // First page button
    if (startPage > 1) {
        pagination.appendChild(createPageButton(1, 'First'));
    }
    
    // Previous page button
    if (state.currentPage > 1) {
        pagination.appendChild(createPageButton(state.currentPage - 1, 'Previous'));
    }
    
    // Page numbers
    for (let i = startPage; i <= endPage; i++) {
        pagination.appendChild(createPageButton(i, i.toString(), i === state.currentPage));
    }
    
    // Next page button
    if (state.currentPage < state.totalPages) {
        pagination.appendChild(createPageButton(state.currentPage + 1, 'Next'));
    }
    
    // Last page button
    if (endPage < state.totalPages) {
        pagination.appendChild(createPageButton(state.totalPages, 'Last'));
    }
}

// Create a pagination button
function createPageButton(page, text, isActive = false) {
    const li = document.createElement('li');
    li.className = `page-item ${isActive ? 'active' : ''}`;
    
    const button = document.createElement('button');
    button.className = 'page-link';
    button.textContent = text;
    button.onclick = () => fetchData(page);
    
    li.appendChild(button);
    return li;
}

// Format currency values
function formatCurrency(value) {
    if (value === null || value === undefined) return '0.00';
    return parseFloat(value).toLocaleString(undefined, {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
    });
}

function renderActiveFilterChips() {
    const $container = $('#activeFilters');
    $container.empty();

    const filterLabels = {
        region: 'Region',
        city: 'City',
        code: 'Code/Description',
        hospital_name: 'Hospital',
        payer_name: 'Payer',
        plan_name: 'Plan'
    };

    // Debug: log current filters
    console.log('Rendering chips for filters:', state.filters);

    Object.entries(state.filters).forEach(([key, value]) => {
        if (value && ((Array.isArray(value) && value.length) || (!Array.isArray(value) && value))) {
            if (Array.isArray(value)) {
                value.forEach(val => {
                    if (val) {
                        $container.append(`
                            <span class="filter-chip" data-filter="${key}" data-value="${val}">
                                ${filterLabels[key] || key}: ${val}
                                <span class="chip-remove" title="Remove">&times;</span>
                            </span>
                        `);
                    }
                });
            } else {
                $container.append(`
                    <span class="filter-chip" data-filter="${key}" data-value="${value}">
                        ${filterLabels[key] || key}: ${value}
                        <span class="chip-remove" title="Remove">&times;</span>
                    </span>
                `);
            }
        }
    });

    // Remove filter on chip click
    $('.filter-chip .chip-remove').on('click', function() {
        const $chip = $(this).closest('.filter-chip');
        const filter = $chip.data('filter');
        const value = $chip.data('value');

        if (Array.isArray(state.filters[filter])) {
            state.filters[filter] = state.filters[filter].filter(v => v !== value);
        } else {
            state.filters[filter] = '';
        }

        $(`#${filter}Filter`).val(state.filters[filter]).trigger('change');
        renderActiveFilterChips();
        fetchData();
    });
} 