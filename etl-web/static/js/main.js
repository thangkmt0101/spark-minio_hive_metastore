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

// ==================== EXPORT CSV FUNCTIONS ====================

function exportToCSV() {
    // Get current filtered jobs
    let dataToExport = allJobs;
    
    // Apply current filters
    if (currentFilter !== 'all') {
        dataToExport = dataToExport.filter(job => job.job_type === currentFilter);
    }
    
    if (currentStatus !== 'all') {
        const isActive = currentStatus === 'active';
        dataToExport = dataToExport.filter(job => job.is_active === isActive);
    }
    
    if (currentSearch) {
        dataToExport = dataToExport.filter(job => {
            const tableName = (job.table_name || '').toLowerCase();
            return tableName.includes(currentSearch);
        });
    }
    
    if (dataToExport.length === 0) {
        showError('Không có dữ liệu để export!');
        return;
    }
    
    // Create CSV header
    const headers = ['ID', 'Loại Job', 'Schema', 'Tên Bảng', 'Đường dẫn SQL', 'Mô tả', 'Trạng thái', 'Ngày tạo', 'Ngày cập nhật'];
    
    // Create CSV rows
    const rows = dataToExport.map(job => [
        job.id,
        `${job.job_type} - ${getJobTypeLabel(job.job_type)}`,
        job.schema_name || '',
        job.table_name || '',
        job.sql_path || '',
        job.description || '',
        job.is_active ? 'Hoạt động' : 'Không hoạt động',
        job.created_at || '',
        job.updated_at || ''
    ]);
    
    // Combine header and rows
    const csvContent = [
        headers.join(','),
        ...rows.map(row => row.map(cell => `"${cell}"`).join(','))
    ].join('\n');
    
    // Add BOM for UTF-8 encoding (Excel compatibility)
    const BOM = '\uFEFF';
    const blob = new Blob([BOM + csvContent], { type: 'text/csv;charset=utf-8;' });
    
    // Create download link
    const link = document.createElement('a');
    const url = URL.createObjectURL(blob);
    
    // Generate filename with timestamp
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, -5);
    const filename = `etl_jobs_${timestamp}.csv`;
    
    link.setAttribute('href', url);
    link.setAttribute('download', filename);
    link.style.display = 'none';
    
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    
    // Show success message
    showSuccess(`Đã export ${dataToExport.length} bản ghi ra file ${filename}`);
}

// ==================== IMPORT EXCEL FUNCTIONS ====================

let selectedFile = null;
let previewData = null;

// Open Import Modal
function openImportModal() {
    document.getElementById('importModal').classList.add('show');
    selectedFile = null;
    previewData = null;
    document.getElementById('excelFile').value = '';
    document.getElementById('fileInfo').style.display = 'none';
    document.getElementById('btnPreview').disabled = true;
}

// Close Import Modal
function closeImportModal() {
    document.getElementById('importModal').classList.remove('show');
}

// Handle File Selection
document.addEventListener('DOMContentLoaded', () => {
    const btnExport = document.getElementById('btnExport');
    const btnImport = document.getElementById('btnImport');
    const excelFileInput = document.getElementById('excelFile');
    const btnPreview = document.getElementById('btnPreview');
    const btnConfirmImport = document.getElementById('btnConfirmImport');
    
    if (btnExport) {
        btnExport.addEventListener('click', exportToCSV);
    }
    
    if (btnImport) {
        btnImport.addEventListener('click', openImportModal);
    }
    
    if (excelFileInput) {
        excelFileInput.addEventListener('change', handleFileSelect);
    }
    
    if (btnPreview) {
        btnPreview.addEventListener('click', previewExcelData);
    }
    
    if (btnConfirmImport) {
        btnConfirmImport.addEventListener('click', confirmImport);
    }
    
    // Drag and drop
    const fileUploadArea = document.getElementById('fileUploadArea');
    if (fileUploadArea) {
        fileUploadArea.addEventListener('dragover', (e) => {
            e.preventDefault();
            fileUploadArea.style.borderColor = 'var(--primary-color)';
            fileUploadArea.style.background = 'var(--light-color)';
        });
        
        fileUploadArea.addEventListener('dragleave', (e) => {
            e.preventDefault();
            fileUploadArea.style.borderColor = '';
            fileUploadArea.style.background = '';
        });
        
        fileUploadArea.addEventListener('drop', (e) => {
            e.preventDefault();
            fileUploadArea.style.borderColor = '';
            fileUploadArea.style.background = '';
            
            const files = e.dataTransfer.files;
            if (files.length > 0) {
                document.getElementById('excelFile').files = files;
                handleFileSelect();
            }
        });
    }
});

function handleFileSelect() {
    const fileInput = document.getElementById('excelFile');
    const file = fileInput.files[0];
    
    if (!file) return;
    
    // Validate file size (5MB max)
    if (file.size > 5 * 1024 * 1024) {
        showError('File quá lớn! Kích thước tối đa 5MB');
        fileInput.value = '';
        return;
    }
    
    // Validate file type
    const validTypes = ['.xlsx', '.xls'];
    const fileExt = '.' + file.name.split('.').pop().toLowerCase();
    if (!validTypes.includes(fileExt)) {
        showError('File không hợp lệ! Chỉ chấp nhận .xlsx, .xls');
        fileInput.value = '';
        return;
    }
    
    selectedFile = file;
    
    // Show file info
    document.getElementById('fileName').textContent = file.name;
    document.getElementById('fileSize').textContent = formatFileSize(file.size);
    document.getElementById('fileInfo').style.display = 'block';
    document.getElementById('btnPreview').disabled = false;
}

function formatFileSize(bytes) {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(2) + ' KB';
    return (bytes / (1024 * 1024)).toFixed(2) + ' MB';
}

async function previewExcelData() {
    if (!selectedFile) {
        showError('Chưa chọn file!');
        return;
    }
    
    const formData = new FormData();
    formData.append('file', selectedFile);
    
    try {
        document.getElementById('btnPreview').disabled = true;
        document.getElementById('btnPreview').innerHTML = '<i class="fas fa-spinner fa-spin"></i> Đang xử lý...';
        
        const response = await fetch(`${API_URL}/import/preview`, {
            method: 'POST',
            body: formData
        });
        
        const result = await response.json();
        
        if (result.success) {
            previewData = result.data;
            showPreviewModal(result.data, result.summary);
            closeImportModal();
        } else {
            showError(result.error || 'Lỗi preview dữ liệu');
        }
    } catch (error) {
        showError('Lỗi kết nối: ' + error.message);
    } finally {
        document.getElementById('btnPreview').disabled = false;
        document.getElementById('btnPreview').innerHTML = '<i class="fas fa-eye"></i> Preview Dữ liệu';
    }
}

function showPreviewModal(data, summary) {
    const modal = document.getElementById('previewModal');
    const tbody = document.getElementById('previewTableBody');
    
    // Update summary
    document.getElementById('totalRecords').textContent = summary.total;
    document.getElementById('validRecords').textContent = summary.valid;
    
    // Show errors if any
    const errorSummary = document.getElementById('errorSummary');
    if (summary.errors && summary.errors.length > 0) {
        errorSummary.style.display = 'block';
        errorSummary.innerHTML = `<strong>Lỗi validation:</strong><br>${summary.errors.join('<br>')}`;
    } else {
        errorSummary.style.display = 'none';
    }
    
    // Render table
    tbody.innerHTML = data.map(row => `
        <tr class="${row.valid ? '' : 'invalid-row'}">
            <td>${row.row_number}</td>
            <td>
                <span class="job-type-badge">
                    ${row.job_type} - ${getJobTypeLabel(row.job_type)}
                </span>
            </td>
            <td>${row.schema_name}</td>
            <td>${row.table_name}</td>
            <td title="${row.sql_path}">${truncate(row.sql_path || '-', 30)}</td>
            <td title="${row.description}">${truncate(row.description || '-', 30)}</td>
            <td>
                <span class="status-badge ${row.is_active ? 'status-active' : 'status-inactive'}">
                    ${row.is_active ? 'Hoạt động' : 'Không hoạt động'}
                </span>
            </td>
            <td>
                ${row.valid 
                    ? '<span style="color: green;"><i class="fas fa-check-circle"></i> Hợp lệ</span>' 
                    : '<span style="color: red;"><i class="fas fa-times-circle"></i> ' + row.errors.join(', ') + '</span>'}
            </td>
        </tr>
    `).join('');
    
    modal.classList.add('show');
}

function closePreviewModal() {
    document.getElementById('previewModal').classList.remove('show');
}

async function confirmImport() {
    if (!previewData) {
        showError('Không có dữ liệu để import!');
        return;
    }
    
    const validData = previewData.filter(r => r.valid);
    
    if (validData.length === 0) {
        showError('Không có bản ghi hợp lệ để import!');
        return;
    }
    
    if (!confirm(`Xác nhận import ${validData.length} bản ghi vào database?`)) {
        return;
    }
    
    try {
        document.getElementById('btnConfirmImport').disabled = true;
        document.getElementById('btnConfirmImport').innerHTML = '<i class="fas fa-spinner fa-spin"></i> Đang import...';
        
        const response = await fetch(`${API_URL}/import/confirm`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ records: previewData })
        });
        
        const result = await response.json();
        
        if (result.success) {
            showSuccess(result.message);
            closePreviewModal();
            loadJobs(); // Reload data
        } else {
            showError(result.error || 'Lỗi import dữ liệu');
        }
    } catch (error) {
        showError('Lỗi kết nối: ' + error.message);
    } finally {
        document.getElementById('btnConfirmImport').disabled = false;
        document.getElementById('btnConfirmImport').innerHTML = '<i class="fas fa-check"></i> Xác nhận Import';
    }
}
