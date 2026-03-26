// API Base URL - Relative URL (tự động lấy port từ server)
const API_URL = '/api';

// DOM Elements - Cached for performance
const DOM = {
    formModal: document.getElementById('formModal'),
    jobForm: document.getElementById('jobForm'),
    btnAdd: document.getElementById('btnAdd'),
    btnConnection: document.getElementById('btnConnection'),
    btnDataActions: document.getElementById('btnDataActions'),
    btnAuditLogs: document.getElementById('btnAuditLogs'),
    btnEtlJobLogs: document.getElementById('btnEtlJobLogs'),
    btnEtlTableMigrateBrandPage: document.getElementById('btnEtlTableMigrateBrandPage'),
    btnCancel: document.getElementById('btnCancel'),
    closeModal: document.querySelector('.close'),
    formTitle: document.getElementById('formTitle'),
    jobTableBody: document.getElementById('jobTableBody'),
    filterCount: document.getElementById('filterCount'),
    searchInput: document.getElementById('searchInput'),
    clearSearchBtn: document.getElementById('clearSearch'),
    statusFilter: document.getElementById('statusFilter'),
    paginationInfo: document.getElementById('paginationInfo'),
    btnPrevPage: document.getElementById('btnPrevPage'),
    btnNextPage: document.getElementById('btnNextPage'),
    pageSizeSelect: document.getElementById('pageSizeSelect'),
    reportTypeFilter: document.getElementById('reportTypeFilter'),
    btnLogin: document.getElementById('btnLogin'),
    btnLogout: document.getElementById('btnLogout'),
    btnUserManage: document.getElementById('btnUserManage'),
    authUserInfo: document.getElementById('authUserInfo')
};

// State
let editingJobId = null;
let allJobs = []; // Lưu trữ toàn bộ jobs
let currentFilter = 'all'; // Filter loại job hiện tại
let currentSearch = ''; // Từ khóa tìm kiếm hiện tại
let currentStatus = 'all'; // Filter trạng thái hiện tại
let currentReportType = 'all'; // Filter loại báo cáo (cắt từ sql_path)

// Pagination (client-side)
let currentPage = 1;
let pageSize = 10;
let filteredJobsCache = [];
let totalPages = 1;
let currentUser = null;
let userListCache = [];

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    setupEventListeners();
    initAuth();
});

// Setup Event Listeners
let searchTimeout;

function setupEventListeners() {
    if (DOM.btnAdd) DOM.btnAdd.addEventListener('click', () => openModal());
    if (DOM.btnConnection && typeof openConnectionModal === 'function') {
        DOM.btnConnection.addEventListener('click', openConnectionModal);
    }
    DOM.btnLogin?.addEventListener('click', openLoginModal);
    DOM.btnLogout?.addEventListener('click', doLogout);
    DOM.btnUserManage?.addEventListener('click', openUserModal);
    DOM.btnDataActions?.addEventListener('click', openDataActionModal);
    DOM.btnAuditLogs?.addEventListener('click', openAuditModal);
    DOM.btnEtlJobLogs?.addEventListener('click', openEtlJobLogModal);
    DOM.btnEtlTableMigrateBrandPage?.addEventListener('click', () => {
        window.location.href = '/etl-table-migrate-brand';
    });
    if (DOM.btnCancel) DOM.btnCancel.addEventListener('click', closeModalForm);
    if (DOM.closeModal) DOM.closeModal.addEventListener('click', closeModalForm);
    if (DOM.jobForm) DOM.jobForm.addEventListener('submit', handleSubmit);
    
    // Filter buttons - Event delegation for better performance
    document.querySelector('.filter-buttons').addEventListener('click', (e) => {
        if (e.target.closest('.filter-btn')) {
            const filter = e.target.closest('.filter-btn').dataset.filter;
            setActiveFilter(filter);
            applyFilters();
        }
    });
    
    // Search with debounce
    DOM.searchInput?.addEventListener('input', (e) => {
        clearTimeout(searchTimeout);
        currentSearch = e.target.value.toLowerCase().trim();
        DOM.clearSearchBtn.style.display = currentSearch ? 'block' : 'none';
        searchTimeout = setTimeout(applyFilters, 300);
    });
    
    // Clear search
    DOM.clearSearchBtn?.addEventListener('click', () => {
        DOM.searchInput.value = '';
        currentSearch = '';
        DOM.clearSearchBtn.style.display = 'none';
        applyFilters();
    });
    
    // Status filter
    DOM.statusFilter?.addEventListener('change', (e) => {
        currentStatus = e.target.value;
        applyFilters();
    });

    DOM.reportTypeFilter?.addEventListener('change', (e) => {
        currentReportType = e.target.value;
        currentPage = 1;
        applyFilters();
    });

    // Pagination controls
    DOM.btnPrevPage?.addEventListener('click', () => {
        if (currentPage <= 1) return;
        currentPage -= 1;
        renderPaginationPage();
    });

    DOM.btnNextPage?.addEventListener('click', () => {
        if (currentPage >= totalPages) return;
        currentPage += 1;
        renderPaginationPage();
    });

    DOM.pageSizeSelect?.addEventListener('change', (e) => {
        const val = parseInt(e.target.value, 10);
        pageSize = Number.isFinite(val) && val > 0 ? val : 10;
        currentPage = 1;
        applyFilters();
    });
    
    // NOTE: Không tự đóng form khi click ra ngoài
    // (tránh mất dữ liệu người dùng đang nhập khi click nhầm)
}

function canManageJobs() {
    return !!currentUser;
}

function canManageUsers() {
    return currentUser && currentUser.role === 'admin';
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
    
    // Filter 3: Tìm kiếm theo tên bảng + tên file SQL
    if (currentSearch) {
        filteredJobs = filteredJobs.filter(job => {
            const tableName = (job.table_name || '').toLowerCase();
            const sqlPath = (job.sql_path || '').replace(/\\/g, '/').toLowerCase();
            const fileName = sqlPath ? sqlPath.split('/').pop() : '';
            const searchNoExt = currentSearch.endsWith('.sql')
                ? currentSearch.slice(0, -4)
                : currentSearch;
            const fileNameNoExt = fileName.endsWith('.sql')
                ? fileName.slice(0, -4)
                : fileName;

            return (
                tableName.includes(currentSearch) ||
                fileName.includes(currentSearch) ||
                fileNameNoExt.includes(searchNoExt)
            );
        });
    }

    // Filter 4: Lọc theo loại báo cáo (cắt từ SQL Path)
    if (currentReportType !== 'all') {
        filteredJobs = filteredJobs.filter(job => {
            const reportType = getReportTypeFromSqlPath(job.sql_path);
            return reportType === currentReportType;
        });
    }

    filteredJobsCache = filteredJobs;
    const totalCount = filteredJobsCache.length;
    totalPages = Math.max(1, Math.ceil(totalCount / pageSize));
    if (currentPage > totalPages) currentPage = totalPages;

    renderPaginationPage();
    updateFilterCount(totalCount);
}

function getReportTypeFromSqlPath(sqlPath) {
    if (!sqlPath) return '';
    const normalized = String(sqlPath).replace(/\\/g, '/').trim();
    // remove leading 'sql/' if present
    const withoutSqlRoot = normalized.replace(/^sql\//i, '');
    const parts = withoutSqlRoot.split('/').filter(Boolean);
    if (parts.length >= 1) {
        // first folder after sql/ is treated as report type
        const first = parts[0];
        if (first && !first.toLowerCase().endsWith('.sql')) return first;
    }
    const fileName = parts.length ? parts[parts.length - 1] : '';
    return fileName ? fileName.replace(/\.sql$/i, '') : '';
}

function populateReportTypeFilterOptions() {
    const select = DOM.reportTypeFilter;
    if (!select) return;
    const types = new Set();

    (allJobs || []).forEach(job => {
        const t = getReportTypeFromSqlPath(job?.sql_path);
        if (t) types.add(t);
    });

    const prev = currentReportType;

    // Build options safely (avoid escaping issues)
    while (select.firstChild) select.removeChild(select.firstChild);

    const optAll = document.createElement('option');
    optAll.value = 'all';
    optAll.textContent = 'Tất cả';
    select.appendChild(optAll);

    Array.from(types).sort((a, b) => String(a).localeCompare(String(b), 'vi')).forEach(t => {
        const opt = document.createElement('option');
        opt.value = String(t);
        opt.textContent = String(t);
        select.appendChild(opt);
    });

    select.value = types.has(prev) ? prev : 'all';
    currentReportType = select.value;
}

function renderPaginationPage() {
    const totalCount = filteredJobsCache.length;
    const start = (currentPage - 1) * pageSize;
    const end = start + pageSize;
    const pageJobs = filteredJobsCache.slice(start, end);

    renderJobs(pageJobs);

    if (DOM.paginationInfo) {
        if (totalCount === 0) {
            DOM.paginationInfo.textContent = 'Trang 0';
        } else {
            DOM.paginationInfo.textContent = `Trang ${currentPage} / ${totalPages} (tổng ${totalCount})`;
        }
    }

    if (DOM.btnPrevPage) DOM.btnPrevPage.disabled = currentPage <= 1;
    if (DOM.btnNextPage) DOM.btnNextPage.disabled = currentPage >= totalPages;
}

// Update Filter Count
function updateFilterCount(count) {
    DOM.filterCount.innerHTML = `Hiển thị: <strong>${count}</strong> job${count > 1 ? 's' : ''}`;
}

// Modal Management
function setJobFormEditMode(editMode) {
    const ids = [
        'group-jobType',
        'group-schemaName',
        'group-tableName',
        'group-sqlPath',
        'group-sqlCommand',
        'group-description',
        'group-isActive'
    ];

    let toShow = ids; // default full
    let title = 'Chỉnh sửa ETL Job';

    if (editMode === 'job_type') {
        toShow = ['group-jobType'];
        title = 'Chỉnh sửa Loại Job';
    } else if (editMode === 'schema_name') {
        toShow = ['group-schemaName'];
        title = 'Chỉnh sửa Tên Schema';
    } else if (editMode === 'table_name') {
        toShow = ['group-tableName'];
        title = 'Chỉnh sửa Tên Bảng';
    } else if (editMode === 'sql_path') {
        // sqlCommand là phụ thuộc trực tiếp theo sql_path
        toShow = ['group-sqlPath', 'group-sqlCommand'];
        title = 'Chỉnh sửa Đường dẫn SQL';
    } else if (editMode === 'is_active') {
        toShow = ['group-isActive'];
        title = 'Chỉnh sửa Trạng thái';
    } else if (editMode === 'full') {
        toShow = ids;
        title = 'Chỉnh sửa ETL Job';
    }

    ids.forEach(id => {
        const el = document.getElementById(id);
        if (!el) return;
        el.style.display = toShow.includes(id) ? '' : 'none';
    });

    return title;
}

function openModal(job = null, editMode = 'full') {
    if (!canManageJobs()) {
        showError('Bạn không có quyền chỉnh sửa dữ liệu');
        return;
    }
    editingJobId = job?.id || null;

    if (!job) {
        setJobFormEditMode('full');
        DOM.formTitle.textContent = 'Thêm ETL Job Mới';
    } else {
        const title = setJobFormEditMode(editMode);
        DOM.formTitle.textContent = title;
    }
    
    if (job) {
        const fields = ['jobId', 'jobType', 'schemaName', 'tableName', 'sqlPath', 'description'];
        const values = [job.id, job.job_type, job.schema_name, job.table_name, job.sql_path, job.description];
        fields.forEach((field, i) => {
            const el = document.getElementById(field);
            if (el) el.value = values[i] || '';
        });
        document.getElementById('isActive').checked = job.is_active;
        if (job.sql_path) {
            loadSqlContentByPath(job.sql_path);
        } else {
            document.getElementById('sqlCommand').value = '';
        }
    } else {
        DOM.jobForm.reset();
        document.getElementById('isActive').checked = true;
        document.getElementById('sqlCommand').value = '';
    }
    
    DOM.formModal.classList.add('show');
}

function closeModalForm() {
    DOM.formModal.classList.remove('show');
    DOM.jobForm.reset();
    editingJobId = null;
    setJobFormEditMode('full');
}

// Load Jobs
async function loadJobs() {
    try {
        const response = await fetch(`${API_URL}/jobs`);
        const result = await response.json();
        
        if (result.success) {
            allJobs = result.data; // Lưu trữ toàn bộ jobs
            populateReportTypeFilterOptions();
            currentPage = 1;
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
    const canEdit = canManageJobs();
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
                    ${canEdit && currentFilter === 'all' && currentStatus === 'all' && !currentSearch ? `
                        <button class="btn btn-primary" onclick="openModal()">
                            <i class="fas fa-plus"></i> Thêm Job Đầu Tiên
                        </button>
                    ` : ''}
                </td>
            </tr>
        `;
        return;
    }
    
    jobTableBody.innerHTML = jobs.map(job => {
        const safeSqlPath = (job.sql_path || '').replace(/\\/g, '\\\\').replace(/'/g, "\\'");
        const safeTable = (job.table_name || '').replace(/\\/g, '\\\\').replace(/'/g, "\\'");
        const safeStatus = job.is_active ? 'Hoạt động' : 'Không hoạt động';
        const safeJobTypeText = `${job.job_type} - ${getJobTypeLabel(job.job_type)}`;
        const reportType = getReportTypeFromSqlPath(job.sql_path);
        const safeReportType = (reportType || '').replace(/\\/g, '\\\\').replace(/'/g, "\\'");

        const editJobTypeAttr = canEdit ? `class="cell-editable" onclick='editJobById(${job.id}, "job_type")'` : '';
        const editTableAttr = canEdit ? `class="cell-editable" onclick='editJobById(${job.id}, "table_name")'` : '';
        const editSqlPathAttr = canEdit ? `class="cell-editable" onclick='editJobById(${job.id}, "sql_path")'` : '';
        const editStatusAttr = canEdit ? `class="cell-editable" onclick='editJobById(${job.id}, "is_active")'` : '';
        const openSqlAttr = canEdit ? `onclick="openSqlEditModal('${safeSqlPath}')"` : '';

        return `
            <tr>
                <td>
                    <div class="cell-with-copy">
                        <span class="cell-value">${job.id}</span>
                        <button class="copy-btn" onclick="copyCellText(event, this, '${String(job.id)}')" title="Copy">
                            <i class="fas fa-copy"></i>
                        </button>
                    </div>
                </td>

                <td ${editJobTypeAttr}>
                    <div class="cell-with-copy">
                        <span class="job-type-badge cell-value">
                            ${job.job_type} - ${getJobTypeLabel(job.job_type)}
                        </span>
                        <button class="copy-btn" onclick="copyCellText(event, this, '${safeJobTypeText.replace(/\\/g,'\\\\').replace(/'/g,"\\'")}')" title="Copy">
                            <i class="fas fa-copy"></i>
                        </button>
                    </div>
                </td>

                <td>
                    <div class="cell-with-copy">
                        <span class="cell-value">${reportType || '-'}</span>
                        <button class="copy-btn" onclick="copyCellText(event, this, '${safeReportType}')" title="Copy">
                            <i class="fas fa-copy"></i>
                        </button>
                    </div>
                </td>

                <td ${editTableAttr}>
                    <div class="cell-with-copy">
                        <span class="cell-value">${job.table_name || '-'}</span>
                        <button class="copy-btn" onclick="copyCellText(event, this, '${safeTable}')" title="Copy">
                            <i class="fas fa-copy"></i>
                        </button>
                    </div>
                </td>

                <td ${editSqlPathAttr}>
                    ${job.sql_path ? `
                        <div class="sql-path-cell cell-with-copy">
                            <span class="sql-path-text"
                                  onmouseenter="showTooltip(event, '${(job.sql_path || '').replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/\\n/g, ' ')}')"
                                  onmouseleave="hideTooltip()"
                                  data-full-path="${job.sql_path}">
                                ${truncate(job.sql_path, 35)}
                            </span>
                            <button class="copy-btn" onclick="copyCellText(event, this, '${safeSqlPath}')" title="Copy">
                                <i class="fas fa-copy"></i>
                            </button>
                        </div>
                    ` : '-'}
                </td>

                <td class="sql-preview-cell cell-with-copy" title="${job.sql_command_preview || ''}"
                    ${openSqlAttr}>
                    <span class="desc-text cell-value">
                        ${job.sql_command_preview ? truncate(job.sql_command_preview, 200) : '-'}
                    </span>
                    <button class="copy-btn" onclick="copySqlContentFromPath(event, this, '${safeSqlPath}')" title="Copy SQL">
                        <i class="fas fa-copy"></i>
                    </button>
                </td>

                <td ${editStatusAttr}>
                    <div class="cell-with-copy">
                        <span class="status-badge ${job.is_active ? 'status-active' : 'status-inactive'} cell-value">
                            ${safeStatus}
                        </span>
                        <button class="copy-btn" onclick="copyCellText(event, this, '${safeStatus.replace(/\\/g,'\\\\').replace(/'/g,"\\'")}')" title="Copy">
                            <i class="fas fa-copy"></i>
                        </button>
                    </div>
                </td>

                <td>
                    <div class="action-buttons">
                        ${canEdit ? `
                            <button class="btn btn-danger btn-sm job-delete-btn" onclick="deleteJob(${job.id})" title="Xóa">
                                <i class="fas fa-trash"></i>
                            </button>
                        ` : '-'}
                    </div>
                </td>
            </tr>
        `;
    }).join('');
}

// ==================== SQL EDIT MODAL ====================
let sqlEditEditor = null;

function initSqlEditEditor() {
    const textarea = document.getElementById('sqlEditContent');
    if (!textarea) return null;
    if (sqlEditEditor) return sqlEditEditor;

    // CodeMirror chỉ sẵn sàng khi CDN đã load
    if (typeof CodeMirror === 'undefined') return null;

    sqlEditEditor = CodeMirror.fromTextArea(textarea, {
        mode: 'text/x-sql',
        lineNumbers: true,
        lineWrapping: false,
        viewportMargin: Infinity,
        styleActiveLine: true,
        theme: 'material-darker'
    });
    return sqlEditEditor;
}

function formatSqlForDisplay(sql) {
    if (sql === undefined || sql === null) return '';
    let s = String(sql).replace(/\r\n/g, '\n').replace(/\r/g, '\n');
    s = s.trim();
    if (!s) return '';

    // Normalize whitespace to make it easier to read
    s = s.replace(/[ \t]+/g, ' ');

    // Put each major clause on a new line
    const keywordRgxs = [
        'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN', 'INNER JOIN', 'OUTER JOIN',
        'GROUP BY', 'ORDER BY',
        'SELECT', 'FROM', 'WHERE', 'HAVING', 'LIMIT',
        'JOIN', 'ON',
        'UNION', 'EXCEPT', 'INTERSECT'
    ];
    const keywordPattern = keywordRgxs
        .map(k => k.replace(/\s+/g, '\\s+'))
        .join('|');

    const rgx = new RegExp(`\\b(${keywordPattern})\\b`, 'gi');
    s = s.replace(rgx, '\n$1');

    // Normalize after semicolon
    s = s.replace(/;\s*/g, ';\n');

    // Trim each line and add minimal indentation
    const lines = s.split('\n').map(l => l.trim()).filter(Boolean);
    let lastClause = '';
    const topClauses = ['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'HAVING', 'LIMIT'];

    const isTopClauseLine = (line) => {
        const up = line.toUpperCase();
        return topClauses.some(c => up.startsWith(c));
    };

    const clauseOfLine = (line) => {
        const up = line.toUpperCase();
        if (up.startsWith('GROUP BY')) return 'GROUP BY';
        if (up.startsWith('ORDER BY')) return 'ORDER BY';
        return up.split(/\s+/)[0];
    };

    const indentOf = (line) => {
        const up = line.toUpperCase();
        if (up.startsWith('AND') || up.startsWith('OR')) return 4;
        if (lastClause === 'SELECT') return 2;
        if (lastClause === 'WHERE') return 2;
        if (up.startsWith('ON')) return 2;
        return 0;
    };

    const outLines = [];
    for (const line of lines) {
        if (isTopClauseLine(line)) {
            lastClause = clauseOfLine(line);
        }
        const ind = indentOf(line);
        outLines.push(' '.repeat(ind) + line);
    }

    return outLines.join('\n');
}

function formatSqlInEditModal() {
    const sqlEditContent = document.getElementById('sqlEditContent');
    const value = sqlEditEditor ? sqlEditEditor.getValue() : (sqlEditContent ? sqlEditContent.value : '');
    let formatted = '';
    try {
        // ưu tiên thư viện sql-formatter (format chuẩn hơn)
        if (window.sqlFormatter && typeof window.sqlFormatter.format === 'function') {
            formatted = window.sqlFormatter.format(value || '', {
                language: 'sql',
                uppercase: true,
                indent: '  '
            });
        } else {
            formatted = formatSqlForDisplay(value || '');
        }
    } catch (e) {
        formatted = formatSqlForDisplay(value || '');
    }

    if (sqlEditEditor) {
        sqlEditEditor.setValue(formatted);
    } else if (sqlEditContent) {
        sqlEditContent.value = formatted;
    }
}

function closeSqlEditModal() {
    const modal = document.getElementById('sqlEditModal');
    if (modal) modal.classList.remove('show');
}

async function openSqlEditModal(sqlPath) {
    const modal = document.getElementById('sqlEditModal');
    const sqlEditSqlPath = document.getElementById('sqlEditSqlPath');
    const sqlEditContent = document.getElementById('sqlEditContent');

    if (!modal || !sqlEditSqlPath || !sqlEditContent) return;
    if (!sqlPath) {
        showError('Không có sql_path để sửa');
        return;
    }

    sqlEditSqlPath.value = sqlPath;
    const editor = initSqlEditEditor();
    if (editor) {
        editor.setValue('');
    } else {
        sqlEditContent.value = '';
    }
    modal.classList.add('show');

    try {
        const response = await fetch(`${API_URL}/sql-content?sql_path=${encodeURIComponent(sqlPath)}`);
        const result = await response.json();

        if (result.success) {
            const content = (result.data && result.data.sql_command) ? result.data.sql_command : '';
            const formatted = formatSqlForDisplay(content);
            if (editor) {
                editor.setValue(formatted);
            } else {
                sqlEditContent.value = formatted;
            }
        } else {
            showError(result.error || 'Không load được nội dung SQL');
        }
    } catch (error) {
        showError('Lỗi kết nối: ' + error.message);
    }
}

async function saveSqlEdit(e) {
    e.preventDefault();

    const sqlEditSqlPath = document.getElementById('sqlEditSqlPath');
    const sqlEditContent = document.getElementById('sqlEditContent');
    const btnSave = document.getElementById('btnSaveSqlEdit');

    if (!sqlEditSqlPath || !sqlEditContent) return;

    const sql_path = sqlEditSqlPath.value;
    const sql_command = sqlEditEditor ? sqlEditEditor.getValue() : sqlEditContent.value;

    try {
        if (btnSave) {
            btnSave.disabled = true;
            btnSave.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Đang lưu...';
        }

        const response = await fetch(`${API_URL}/sql-content`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ sql_path, sql_command })
        });

        const result = await response.json();

        if (result.success) {
            showSuccess(result.message);
            closeSqlEditModal();
            loadJobs();
        } else {
            showError(result.error || 'Lỗi khi lưu nội dung SQL');
        }
    } catch (error) {
        showError('Lỗi kết nối: ' + error.message);
    } finally {
        if (btnSave) {
            btnSave.disabled = false;
            btnSave.innerHTML = '<i class="fas fa-save"></i> Lưu';
        }
    }
}

// Handle Form Submit
async function handleSubmit(e) {
    e.preventDefault();
    if (!canManageJobs()) {
        showError('Bạn không có quyền thực hiện thao tác này');
        return;
    }
    
    const formData = {
        job_type: document.getElementById('jobType').value,
        schema_name: document.getElementById('schemaName').value,
        table_name: document.getElementById('tableName').value,
        sql_path: document.getElementById('sqlPath').value || null,
        sql_command: document.getElementById('sqlCommand').value,
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
    openModal(job, 'full');
}

function editJobById(jobId, editMode = 'full') {
    const idStr = jobId === undefined || jobId === null ? '' : String(jobId);
    const job = allJobs.find(j => String(j.id) === idStr);
    openModal(job || null, editMode);
}

// Delete Job
async function deleteJob(jobId) {
    if (!canManageJobs()) {
        showError('Bạn không có quyền xóa');
        return;
    }
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

// ==================== AUTH & USER MANAGEMENT ====================
async function initAuth() {
    try {
        const response = await fetch(`${API_URL}/auth/me`);
        const result = await response.json();
        if (result.success) {
            currentUser = result.data;
            applyAuthUI();
            loadJobs();
            return;
        }
    } catch (error) {
        // ignore
    }
    currentUser = null;
    applyAuthUI();
    document.getElementById('jobTableBody').innerHTML = `
        <tr><td colspan="8" class="empty-state"><p>Vui lòng đăng nhập để sử dụng hệ thống</p></td></tr>
    `;
}

function applyAuthUI() {
    const isLoggedIn = !!currentUser;
    const isAdmin = isLoggedIn && currentUser.role === 'admin';

    if (DOM.btnLogin) DOM.btnLogin.style.display = isLoggedIn ? 'none' : '';
    if (DOM.btnLogout) DOM.btnLogout.style.display = isLoggedIn ? '' : 'none';
    if (DOM.btnAdd) DOM.btnAdd.style.display = isLoggedIn ? '' : 'none';
    if (DOM.btnConnection) DOM.btnConnection.style.display = isAdmin ? '' : 'none';
    if (DOM.btnDataActions) DOM.btnDataActions.style.display = isLoggedIn ? '' : 'none';
    if (DOM.btnAuditLogs) DOM.btnAuditLogs.style.display = isAdmin ? '' : 'none';
    if (DOM.btnEtlJobLogs) DOM.btnEtlJobLogs.style.display = isLoggedIn ? '' : 'none';
    if (DOM.btnEtlTableMigrateBrandPage) DOM.btnEtlTableMigrateBrandPage.style.display = isAdmin ? '' : 'none';
    if (DOM.btnUserManage) DOM.btnUserManage.style.display = isAdmin ? '' : 'none';

    if (DOM.authUserInfo) {
        if (isLoggedIn) {
            const displayName = currentUser.full_name || currentUser.username;
            DOM.authUserInfo.style.display = '';
            DOM.authUserInfo.textContent = `${displayName} (${currentUser.role})`;
        } else {
            DOM.authUserInfo.style.display = 'none';
            DOM.authUserInfo.textContent = '';
        }
    }
}

function openLoginModal() {
    document.getElementById('loginModal')?.classList.add('show');
}

function closeLoginModal() {
    document.getElementById('loginModal')?.classList.remove('show');
    document.getElementById('loginForm')?.reset();
}

async function doLogin(e) {
    e.preventDefault();
    const username = document.getElementById('loginUsername').value.trim();
    const password = document.getElementById('loginPassword').value;

    try {
        const response = await fetch(`${API_URL}/auth/login`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, password })
        });
        const result = await response.json();
        if (!result.success) {
            showError(result.error || 'Đăng nhập thất bại');
            return;
        }
        currentUser = result.data;
        applyAuthUI();
        closeLoginModal();
        showSuccess(result.message || 'Đăng nhập thành công');
        loadJobs();
    } catch (error) {
        showError('Lỗi kết nối: ' + error.message);
    }
}

async function doLogout() {
    try {
        const response = await fetch(`${API_URL}/auth/logout`, { method: 'POST' });
        const result = await response.json();
        currentUser = null;
        applyAuthUI();
        allJobs = [];
        filteredJobsCache = [];
        renderJobs([]);
        showSuccess(result.message || 'Đăng xuất thành công');
    } catch (error) {
        showError('Lỗi kết nối: ' + error.message);
    }
}

function resetUserForm() {
    document.getElementById('userForm')?.reset();
    document.getElementById('userId').value = '';
    document.getElementById('userIsActive').checked = true;
    document.getElementById('userPassword').required = true;
    const requiredLabel = document.getElementById('userPasswordRequired');
    if (requiredLabel) requiredLabel.style.display = '';
}

function fillUserForm(user) {
    document.getElementById('userId').value = user.id;
    document.getElementById('userUsername').value = user.username || '';
    document.getElementById('userFullName').value = user.full_name || '';
    document.getElementById('userRole').value = user.role || 'user';
    document.getElementById('userIsActive').checked = !!user.is_active;
    document.getElementById('userPassword').value = '';
    document.getElementById('userPassword').required = false;
    const requiredLabel = document.getElementById('userPasswordRequired');
    if (requiredLabel) requiredLabel.style.display = 'none';
}

function renderUserTable() {
    const tbody = document.getElementById('userTableBody');
    if (!tbody) return;
    if (!userListCache.length) {
        tbody.innerHTML = '<tr><td colspan="6" class="empty-state">Chưa có user</td></tr>';
        return;
    }
    tbody.innerHTML = userListCache.map(u => `
        <tr>
            <td>${u.id}</td>
            <td>${u.username || ''}</td>
            <td>${u.full_name || ''}</td>
            <td>${u.role || ''}</td>
            <td>${u.is_active ? 'Hoạt động' : 'Khóa'}</td>
            <td>
                <button class="btn btn-secondary btn-sm" onclick="editUserById(${u.id})"><i class="fas fa-pen"></i></button>
                ${String(u.username || '').toLowerCase() === 'admin' ? '' : `
                    <button class="btn btn-danger btn-sm" onclick="deleteUserById(${u.id})"><i class="fas fa-trash"></i></button>
                `}
            </td>
        </tr>
    `).join('');
}

async function loadUsers() {
    const response = await fetch(`${API_URL}/users`);
    const result = await response.json();
    if (!result.success) throw new Error(result.error || 'Không tải được user');
    userListCache = result.data || [];
    renderUserTable();
}

async function openUserModal() {
    if (!canManageUsers()) {
        showError('Bạn không có quyền quản lý user');
        return;
    }
    document.getElementById('userModal')?.classList.add('show');
    resetUserForm();
    try {
        await loadUsers();
    } catch (error) {
        showError(error.message);
    }
}

function closeUserModal() {
    document.getElementById('userModal')?.classList.remove('show');
}

function editUserById(userId) {
    const user = userListCache.find(u => String(u.id) === String(userId));
    if (!user) return;
    fillUserForm(user);
}

async function submitUserForm(e) {
    e.preventDefault();
    if (!canManageUsers()) {
        showError('Bạn không có quyền quản lý user');
        return;
    }
    const userId = document.getElementById('userId').value;
    const payload = {
        username: document.getElementById('userUsername').value.trim(),
        full_name: document.getElementById('userFullName').value.trim(),
        password: document.getElementById('userPassword').value,
        role: document.getElementById('userRole').value,
        is_active: document.getElementById('userIsActive').checked
    };

    if (!userId && !payload.password) {
        showError('Mật khẩu là bắt buộc khi tạo mới user');
        return;
    }

    const url = userId ? `${API_URL}/users/${userId}` : `${API_URL}/users`;
    const method = userId ? 'PUT' : 'POST';

    try {
        const response = await fetch(url, {
            method,
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        const result = await response.json();
        if (!result.success) {
            showError(result.error || 'Lưu user thất bại');
            return;
        }
        showSuccess(result.message || 'Lưu user thành công');
        resetUserForm();
        await loadUsers();
    } catch (error) {
        showError('Lỗi kết nối: ' + error.message);
    }
}

async function deleteUserById(userId) {
    if (!confirm('Bạn có chắc chắn muốn xóa user này?')) return;
    try {
        const response = await fetch(`${API_URL}/users/${userId}`, { method: 'DELETE' });
        const result = await response.json();
        if (!result.success) {
            showError(result.error || 'Xóa user thất bại');
            return;
        }
        showSuccess(result.message || 'Xóa user thành công');
        await loadUsers();
    } catch (error) {
        showError('Lỗi kết nối: ' + error.message);
    }
}

function openDataActionModal() {
    document.getElementById('dataActionModal')?.classList.add('show');
}

function closeDataActionModal() {
    document.getElementById('dataActionModal')?.classList.remove('show');
}

function closeAuditModal() {
    document.getElementById('auditModal')?.classList.remove('show');
}

async function deleteAuditLogs() {
    if (!confirm('Bạn có chắc chắn muốn xóa toàn bộ lịch sử thao tác người dùng?')) return;
    try {
        const response = await fetch(`${API_URL}/audit-logs`, { method: 'DELETE' });
        const result = await response.json();
        if (!result.success) {
            showError(result.error || 'Không thể xóa lịch sử thao tác');
            return;
        }
        showSuccess(result.message || 'Đã xóa lịch sử thao tác');
        openAuditModal();
    } catch (error) {
        showError('Lỗi kết nối: ' + error.message);
    }
}

function renderAuditLogs(logs = []) {
    const tbody = document.getElementById('auditTableBody');
    if (!tbody) return;
    if (!logs.length) {
        tbody.innerHTML = '<tr><td colspan="5" class="empty-state">Chưa có lịch sử thao tác</td></tr>';
        return;
    }
    tbody.innerHTML = logs.map(item => `
        <tr>
            <td>${item.created_at || '-'}</td>
            <td>${item.username || '-'}</td>
            <td>${item.action || '-'}</td>
            <td>${item.status || '-'}</td>
            <td>${item.detail || '-'}</td>
        </tr>
    `).join('');
}

async function openAuditModal() {
    const modal = document.getElementById('auditModal');
    const tbody = document.getElementById('auditTableBody');
    if (!modal || !tbody) return;
    modal.classList.add('show');
    tbody.innerHTML = '<tr><td colspan="5" class="loading">Đang tải...</td></tr>';
    try {
        const response = await fetch(`${API_URL}/audit-logs?limit=200`);
        const result = await response.json();
        if (!result.success) {
            showError(result.error || 'Không tải được lịch sử thao tác');
            return;
        }
        renderAuditLogs(result.data || []);
    } catch (error) {
        showError('Lỗi kết nối: ' + error.message);
    }
}

function closeEtlJobLogModal() {
    document.getElementById('etlJobLogModal')?.classList.remove('show');
}

function renderEtlJobLogs(logs = []) {
    const tbody = document.getElementById('etlJobLogTableBody');
    if (!tbody) return;
    if (!logs.length) {
        tbody.innerHTML = '<tr><td colspan="10" class="empty-state">Chưa có lịch sử ETL Job</td></tr>';
        return;
    }
    tbody.innerHTML = logs.map(item => `
        <tr>
            <td>${item.id ?? '-'}</td>
            <td>${item.created_at || '-'}</td>
            <td>${item.job_name || '-'}</td>
            <td>${item.table_name || '-'}</td>
            <td>${item.operation_type || '-'}</td>
            <td>${item.error_level || '-'}</td>
            <td title="${(item.message || '').replace(/"/g, '&quot;')}">${truncate(item.message || '-', 120)}</td>
            <td>${item.rows_inserted ?? '-'}</td>
            <td>${item.execution_time_ms ?? '-'}</td>
            <td>${item.created_by || '-'}</td>
        </tr>
    `).join('');
}

async function openEtlJobLogModal() {
    window.location.href = '/etl-job-logs';
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

// ==================== CONNECTION CONFIG ====================
async function openConnectionModal() {
    const modal = document.getElementById('connectionModal');
    if (!modal) return;

    try {
        const response = await fetch(`${API_URL}/connection`);
        const result = await response.json();

        if (!result.success) {
            showError(result.error || 'Không thể tải cấu hình connection');
            return;
        }

        document.getElementById('connHost').value = result.data.host || '';
        document.getElementById('connPort').value = result.data.port || '';
        document.getElementById('connDatabase').value = result.data.database || '';
        document.getElementById('connUser').value = result.data.user || '';
        document.getElementById('connPassword').value = result.data.password || '';
        modal.classList.add('show');
    } catch (error) {
        showError('Lỗi kết nối: ' + error.message);
    }
}

function closeConnectionModal() {
    const modal = document.getElementById('connectionModal');
    if (modal) modal.classList.remove('show');
}

async function saveConnectionConfig(e) {
    e.preventDefault();

    const payload = {
        host: document.getElementById('connHost').value.trim(),
        port: document.getElementById('connPort').value.trim(),
        database: document.getElementById('connDatabase').value.trim(),
        user: document.getElementById('connUser').value.trim(),
        password: document.getElementById('connPassword').value
    };

    const btn = document.getElementById('btnSaveConnection');

    try {
        if (btn) {
            btn.disabled = true;
            btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Đang lưu...';
        }

        const response = await fetch(`${API_URL}/connection`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        const result = await response.json();

        if (result.success) {
            showSuccess(result.message || 'Cập nhật connection thành công');
            if (result.warning) {
                alert('! ' + result.warning);
            }
            closeConnectionModal();
            loadJobs();
        } else {
            showError(result.error || 'Lỗi cập nhật connection');
        }
    } catch (error) {
        showError('Lỗi kết nối: ' + error.message);
    } finally {
        if (btn) {
            btn.disabled = false;
            btn.innerHTML = '<i class="fas fa-save"></i> Lưu Connection';
        }
    }
}

async function copyCellText(e, btn, text) {
    e?.stopPropagation();
    if (!btn) return;

    const value = text === undefined || text === null ? '' : String(text);
    const originalHtml = btn.innerHTML;

    try {
        if (navigator.clipboard && navigator.clipboard.writeText) {
            await navigator.clipboard.writeText(value);
            btn.innerHTML = '<i class="fas fa-check"></i>';
            btn.style.color = '#2ecc71';

            setTimeout(() => {
                btn.innerHTML = originalHtml;
                btn.style.color = '';
            }, 1200);
        } else {
            copyPathFallback(value, btn);
        }
    } catch (err) {
        // Fallback if clipboard API fails
        copyPathFallback(value, btn);
    }
}

async function copySqlContentFromPath(e, btn, sqlPath) {
    e?.stopPropagation();
    if (!btn) return;
    if (!sqlPath) {
        showError('Thiếu sql_path để copy SQL');
        return;
    }

    const originalHtml = btn.innerHTML;
    try {
        const response = await fetch(`${API_URL}/sql-content?sql_path=${encodeURIComponent(sqlPath)}`);
        const result = await response.json();

        if (!result.success) {
            showError(result.error || 'Không tải được nội dung SQL');
            return;
        }

        const sqlText = (result.data && result.data.sql_command) ? result.data.sql_command : '';

        if (navigator.clipboard && navigator.clipboard.writeText) {
            await navigator.clipboard.writeText(sqlText);
            btn.innerHTML = '<i class="fas fa-check"></i>';
            btn.style.color = '#2ecc71';

            setTimeout(() => {
                btn.innerHTML = originalHtml;
                btn.style.color = '';
            }, 1200);
        } else {
            copyPathFallback(sqlText, btn);
        }
    } catch (err) {
        copyPathFallback('', btn);
    }
}

async function loadSqlContentByPath(path) {
    try {
        const response = await fetch(`${API_URL}/sql-content?sql_path=${encodeURIComponent(path)}`);
        const result = await response.json();
        if (result.success) {
            document.getElementById('sqlCommand').value = result.data.sql_command || '';
        } else {
            showError(result.error || 'Không load được nội dung SQL');
        }
    } catch (error) {
        showError('Lỗi kết nối: ' + error.message);
    }
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
    const headers = ['ID', 'Loại Job', 'Loại báo cáo', 'Tên Bảng', 'Đường dẫn SQL', 'Mô tả', 'Trạng thái', 'Ngày tạo', 'Ngày cập nhật'];
    
    // Create CSV rows
    const rows = dataToExport.map(job => [
        job.id,
        `${job.job_type} - ${getJobTypeLabel(job.job_type)}`,
        getReportTypeFromSqlPath(job.sql_path),
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
    const btnExportFromMenu = document.getElementById('btnExportFromMenu');
    const btnImportFromMenu = document.getElementById('btnImportFromMenu');
    const excelFileInput = document.getElementById('excelFile');
    const btnPreview = document.getElementById('btnPreview');
    const btnConfirmImport = document.getElementById('btnConfirmImport');
    const sqlEditForm = document.getElementById('sqlEditForm');
    const sqlPathInput = document.getElementById('sqlPath');
    const btnLoadSql = document.getElementById('btnLoadSql');
    const btnFormatSqlEdit = document.getElementById('btnFormatSqlEdit');
    const connectionForm = document.getElementById('connectionForm');
    const loginForm = document.getElementById('loginForm');
    const userForm = document.getElementById('userForm');
    const btnDeleteAuditLogs = document.getElementById('btnDeleteAuditLogs');
    
    if (btnExportFromMenu) {
        btnExportFromMenu.addEventListener('click', () => {
            closeDataActionModal();
            exportToCSV();
        });
    }
    
    if (btnImportFromMenu) {
        btnImportFromMenu.addEventListener('click', () => {
            closeDataActionModal();
            openImportModal();
        });
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

    if (sqlEditForm) {
        sqlEditForm.addEventListener('submit', saveSqlEdit);
    }

    if (sqlPathInput) {
        sqlPathInput.addEventListener('blur', () => {
            const sqlPath = sqlPathInput.value.trim();
            if (sqlPath) {
                loadSqlContentByPath(sqlPath);
            }
        });
    }

    if (btnLoadSql) {
        btnLoadSql.addEventListener('click', () => {
            const sqlPath = document.getElementById('sqlPath').value.trim();
            if (!sqlPath) {
                showError('Vui lòng nhập Đường dẫn SQL trước');
                return;
            }
            loadSqlContentByPath(sqlPath);
        });
    }

    if (btnFormatSqlEdit) {
        btnFormatSqlEdit.addEventListener('click', () => {
            formatSqlInEditModal();
        });
    }

    if (connectionForm) {
        connectionForm.addEventListener('submit', saveConnectionConfig);
    }

    if (loginForm) {
        loginForm.addEventListener('submit', doLogin);
    }

    if (userForm) {
        userForm.addEventListener('submit', submitUserForm);
    }

    if (btnDeleteAuditLogs) {
        btnDeleteAuditLogs.addEventListener('click', deleteAuditLogs);
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
