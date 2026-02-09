// API Base URL - Relative URL (tự động lấy port từ server)
const API_URL = '/api';

// DOM Elements - Cached for performance
const DOM = {
    formModal: document.getElementById('formModal'),
    jobForm: document.getElementById('jobForm'),
    btnAdd: document.getElementById('btnAdd'),
    btnCancel: document.getElementById('btnCancel'),
    closeModal: document.querySelector('.close'),
    formTitle: document.getElementById('formTitle'),
    jobTableBody: document.getElementById('jobTableBody'),
    filterCount: document.getElementById('filterCount'),
    searchInput: document.getElementById('searchInput'),
    clearSearchBtn: document.getElementById('clearSearch'),
    statusFilter: document.getElementById('statusFilter')
};

// State
let editingJobId = null;
let allJobs = []; // Lưu trữ toàn bộ jobs
let currentFilter = 'all'; // Filter loại job hiện tại
let currentSearch = ''; // Từ khóa tìm kiếm hiện tại
let currentStatus = 'all'; // Filter trạng thái hiện tại

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    loadJobs();
    setupEventListeners();
});

// Setup Event Listeners
let searchTimeout;

function setupEventListeners() {
    DOM.btnAdd.addEventListener('click', () => openModal());
    DOM.btnCancel.addEventListener('click', closeModalForm);
    DOM.closeModal.addEventListener('click', closeModalForm);
    DOM.jobForm.addEventListener('submit', handleSubmit);
    
    // Filter buttons - Event delegation for better performance
    document.querySelector('.filter-buttons').addEventListener('click', (e) => {
        if (e.target.closest('.filter-btn')) {
            const filter = e.target.closest('.filter-btn').dataset.filter;
            setActiveFilter(filter);
            applyFilters();
        }
    });
    
    // Search with debounce
    DOM.searchInput.addEventListener('input', (e) => {
        clearTimeout(searchTimeout);
        currentSearch = e.target.value.toLowerCase().trim();
        DOM.clearSearchBtn.style.display = currentSearch ? 'block' : 'none';
        searchTimeout = setTimeout(applyFilters, 300);
    });
    
    // Clear search
    DOM.clearSearchBtn.addEventListener('click', () => {
        DOM.searchInput.value = '';
        currentSearch = '';
        DOM.clearSearchBtn.style.display = 'none';
        applyFilters();
    });
    
    // Status filter
    DOM.statusFilter.addEventListener('change', (e) => {
        currentStatus = e.target.value;
        applyFilters();
    });
    
    // Close modal on outside click
    window.addEventListener('click', (e) => {
        if (e.target === DOM.formModal) closeModalForm();
    });
}

// Set Active Filter Button
function setActiveFilter(filter) {
    currentFilter = filter;
    const filterButtons = document.querySelectorAll('.filter-btn');
    filterButtons.forEach(btn => {
        if (btn.dataset.filter === filter) {
            btn.classList.add('active');
        } else {
            btn.classList.remove('active');
        }
    });
}

// Apply All Filters
function applyFilters() {
    let filteredJobs = allJobs;
    
    // Filter 1: Lọc theo loại job
    if (currentFilter !== 'all') {
        filteredJobs = filteredJobs.filter(job => job.job_type === currentFilter);
    }
    
    // Filter 2: Lọc theo trạng thái
    if (currentStatus !== 'all') {
        const isActive = currentStatus === 'active';
        filteredJobs = filteredJobs.filter(job => job.is_active === isActive);
    }
    
    // Filter 3: Tìm kiếm theo tên bảng
    if (currentSearch) {
        filteredJobs = filteredJobs.filter(job => {
            const tableName = (job.table_name || '').toLowerCase();
            return tableName.includes(currentSearch);
        });
    }
    
    renderJobs(filteredJobs);
    updateFilterCount(filteredJobs.length);
}

// Update Filter Count
function updateFilterCount(count) {
    DOM.filterCount.innerHTML = `Hiển thị: <strong>${count}</strong> job${count > 1 ? 's' : ''}`;
}

// Modal Management
function openModal(job = null) {
    editingJobId = job?.id || null;
    DOM.formTitle.textContent = job ? 'Chỉnh sửa ETL Job' : 'Thêm ETL Job Mới';
    
    if (job) {
        const fields = ['jobId', 'jobType', 'schemaName', 'tableName', 'sqlPath', 'description'];
        const values = [job.id, job.job_type, job.schema_name, job.table_name, job.sql_path, job.description];
        fields.forEach((field, i) => {
            const el = document.getElementById(field);
            if (el) el.value = values[i] || '';
        });
        document.getElementById('isActive').checked = job.is_active;
    } else {
        DOM.jobForm.reset();
        document.getElementById('isActive').checked = true;
    }
    
    DOM.formModal.classList.add('show');
}

function closeModalForm() {
    DOM.formModal.classList.remove('show');
    DOM.jobForm.reset();
    editingJobId = null;
}

// Load Jobs
async function loadJobs() {
    try {
        const response = await fetch(`${API_URL}/jobs`);
        const result = await response.json();
        
        if (result.success) {
            allJobs = result.data; // Lưu trữ toàn bộ jobs
            applyFilters(); // Áp dụng tất cả filters hiện tại
        } else {
            showError('Không thể tải dữ liệu: ' + result.error);
        }
    } catch (error) {
        showError('Lỗi kết nối: ' + error.message);
    }
}

// Render Jobs Table
function renderJobs(jobs) {
    if (jobs.length === 0) {
        let message = 'Chưa có dữ liệu';
        
        // Custom message based on filters
        if (currentSearch) {
            message = `Không tìm thấy bảng "${currentSearch}"`;
        } else if (currentStatus !== 'all') {
            const statusText = currentStatus === 'active' ? 'Hoạt động' : 'Không hoạt động';
            message = `Không có job ${statusText}`;
            if (currentFilter !== 'all') {
                message += ` loại ${getJobTypeLabel(currentFilter)}`;
            }
        } else if (currentFilter !== 'all') {
            message = `Không có job loại ${getJobTypeLabel(currentFilter)}`;
        }
        
        jobTableBody.innerHTML = `
            <tr>
                <td colspan="8" class="empty-state">
                    <i class="fas fa-inbox"></i>
                    <p>${message}</p>
                    ${currentFilter === 'all' && currentStatus === 'all' && !currentSearch ? `
                        <button class="btn btn-primary" onclick="openModal()">
                            <i class="fas fa-plus"></i> Thêm Job Đầu Tiên
                        </button>
                    ` : ''}
                </td>
            </tr>
        `;
        return;
    }
    
    jobTableBody.innerHTML = jobs.map(job => `
        <tr>
            <td>${job.id}</td>
            <td>
                <span class="job-type-badge">
                    ${job.job_type} - ${getJobTypeLabel(job.job_type)}
                </span>
            </td>
            <td>${job.schema_name || '-'}</td>
            <td>${job.table_name || '-'}</td>
            <td>
                ${job.sql_path ? `
                    <div class="sql-path-cell">
                        <span class="sql-path-text" 
                              onclick="showSqlPathModal('${job.sql_path.replace(/'/g, "\\'")}')"
                              onmouseenter="showTooltip(event, '${job.sql_path.replace(/'/g, "\\'").replace(/\n/g, ' ')}')"
                              onmouseleave="hideTooltip()"
                              data-full-path="${job.sql_path}">
                            ${truncate(job.sql_path, 35)}
                        </span>
                        <button class="copy-btn" onclick="copySqlPath(this, '${job.sql_path.replace(/'/g, "\\'")}')" title="Copy">
                            <i class="fas fa-copy"></i>
                        </button>
                    </div>
                ` : '-'}
            </td>
            <td title="${job.description || ''}">${job.description ? truncate(job.description, 40) : '-'}</td>
            <td>
                <span class="status-badge ${job.is_active ? 'status-active' : 'status-inactive'}">
                    ${job.is_active ? 'Hoạt động' : 'Không hoạt động'}
                </span>
            </td>
            <td>
                <div class="action-buttons">
                    <button class="btn btn-warning btn-sm" onclick='editJob(${JSON.stringify(job)})'>
                        <i class="fas fa-edit"></i> Sửa
                    </button>
                    <button class="btn btn-danger btn-sm" onclick="deleteJob(${job.id})">
                        <i class="fas fa-trash"></i> Xóa
                    </button>
                </div>
            </td>
        </tr>
    `).join('');
}

// Handle Form Submit
async function handleSubmit(e) {
    e.preventDefault();
    
    const formData = {
        job_type: document.getElementById('jobType').value,
        schema_name: document.getElementById('schemaName').value,
        table_name: document.getElementById('tableName').value,
        sql_path: document.getElementById('sqlPath').value || null,
        batch_size: null,
        delete_column: null,
        delete_condition: null,
        description: document.getElementById('description').value || null,
        is_active: document.getElementById('isActive').checked
    };
    
    try {
        const url = editingJobId ? `${API_URL}/jobs/${editingJobId}` : `${API_URL}/jobs`;
        const method = editingJobId ? 'PUT' : 'POST';
        
        const response = await fetch(url, {
            method: method,
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(formData)
        });
        
        const result = await response.json();
        
        if (result.success) {
            showSuccess(result.message);
            closeModalForm();
            loadJobs();
        } else {
            showError('Lỗi: ' + result.error);
        }
    } catch (error) {
        showError('Lỗi kết nối: ' + error.message);
    }
}

// Edit Job
function editJob(job) {
    openModal(job);
}

// Delete Job
async function deleteJob(jobId) {
    if (!confirm('Bạn có chắc chắn muốn xóa job này?')) {
        return;
    }
    
    try {
        const response = await fetch(`${API_URL}/jobs/${jobId}`, {
            method: 'DELETE'
        });
        
        const result = await response.json();
        
        if (result.success) {
            showSuccess(result.message);
            loadJobs();
        } else {
            showError('Lỗi: ' + result.error);
        }
    } catch (error) {
        showError('Lỗi kết nối: ' + error.message);
    }
}

// Utility Functions
function truncate(str, length) {
    return str.length > length ? str.substring(0, length) + '...' : str;
}

function getJobTypeLabel(jobType) {
    const jobTypes = {
        '1': 'Daily',
        '2': 'Weekly',
        '3': 'Month',
        '4': 'Quarter',
        '5': 'Year'
    };
    return jobTypes[jobType] || jobType;
}

function copySqlPath(btn, path) {
    event?.stopPropagation();
    navigator.clipboard.writeText(path).then(() => {
        const orig = btn.innerHTML;
        btn.innerHTML = '<i class="fas fa-check"></i>';
        btn.style.color = '#2ecc71';
        setTimeout(() => {
            btn.innerHTML = orig;
            btn.style.color = '';
        }, 1500);
    }).catch(() => alert('Không thể copy'));
}

function showSqlPathModal(path) {
    const modal = document.getElementById('sqlPathModal');
    const pathText = document.getElementById('sqlPathFullText');
    
    pathText.textContent = path;
    pathText.dataset.path = path; // Store for copy
    modal.classList.add('show');
}

function closeSqlPathModal() {
    const modal = document.getElementById('sqlPathModal');
    modal.classList.remove('show');
}

function copySqlPathFromModal(btn) {
    const pathText = document.getElementById('sqlPathFullText');
    const path = pathText.dataset.path || pathText.textContent;
    
    if (!path) {
        alert('Không có path để copy');
        return;
    }
    
    // Try clipboard API
    if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(path).then(() => {
            const originalText = btn.innerHTML;
            btn.innerHTML = '<i class="fas fa-check"></i> Đã copy!';
            btn.style.background = '#2ecc71';
            
            setTimeout(() => {
                btn.innerHTML = originalText;
                btn.style.background = '';
            }, 2000);
        }).catch(err => {
            console.error('Clipboard error:', err);
            // Fallback: Use old method
            copyPathFallback(path, btn);
        });
    } else {
        // Fallback for old browsers
        copyPathFallback(path, btn);
    }
}

// Fallback copy method
function copyPathFallback(text, btn) {
    const textArea = document.createElement('textarea');
    textArea.value = text;
    textArea.style.position = 'fixed';
    textArea.style.left = '-999999px';
    document.body.appendChild(textArea);
    textArea.select();
    
    try {
        document.execCommand('copy');
        const originalText = btn.innerHTML;
        btn.innerHTML = '<i class="fas fa-check"></i> Đã copy!';
        btn.style.background = '#2ecc71';
        
        setTimeout(() => {
            btn.innerHTML = originalText;
            btn.style.background = '';
        }, 2000);
    } catch (err) {
        alert('Không thể copy: ' + err.message);
    } finally {
        document.body.removeChild(textArea);
    }
}

// Floating Tooltip
let floatingTooltip = null;

function showTooltip(event, text) {
    // Create tooltip if not exists
    if (!floatingTooltip) {
        floatingTooltip = document.createElement('div');
        floatingTooltip.id = 'floatingTooltip';
        floatingTooltip.className = 'floating-tooltip';
        document.body.appendChild(floatingTooltip);
    }
    
    // Set content
    floatingTooltip.textContent = text;
    floatingTooltip.style.display = 'block';
    
    // Position tooltip
    const rect = event.target.getBoundingClientRect();
    floatingTooltip.style.left = rect.left + 'px';
    floatingTooltip.style.top = (rect.top - floatingTooltip.offsetHeight - 10) + 'px';
    
    // Show with animation
    setTimeout(() => {
        floatingTooltip.style.opacity = '1';
    }, 10);
}

function hideTooltip() {
    if (floatingTooltip) {
        floatingTooltip.style.opacity = '0';
        setTimeout(() => {
            floatingTooltip.style.display = 'none';
        }, 150);
    }
}

function showSuccess(message) {
    alert('✓ ' + message);
}

function showError(message) {
    alert('✗ ' + message);
}
