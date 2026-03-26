const API_URL = '/api';

const DOM = {
    authUserInfo: document.getElementById('authUserInfo'),
    btnBackDashboard: document.getElementById('btnBackDashboard'),
    btnDeleteLogsByDate: document.getElementById('btnDeleteLogsByDate'),
    btnLogout: document.getElementById('btnLogout'),
    tableSearchInput: document.getElementById('tableSearchInput'),
    btnClearTableSearch: document.getElementById('btnClearTableSearch'),
    btnRefreshLogs: document.getElementById('btnRefreshLogs'),
    pageSizeSelect: document.getElementById('pageSizeSelect'),
    btnPrevPage: document.getElementById('btnPrevPage'),
    btnNextPage: document.getElementById('btnNextPage'),
    paginationInfo: document.getElementById('paginationInfo'),
    etlJobLogPageBody: document.getElementById('etlJobLogPageBody'),
    fullLogModal: document.getElementById('fullLogModal'),
    fullLogMeta: document.getElementById('fullLogMeta'),
    fullLogContent: document.getElementById('fullLogContent')
};

let searchTimeout;
let currentPage = 1;
let totalPages = 1;
let pageSize = 50;
let currentLogs = [];

function truncate(text, length) {
    const value = text || '';
    return value.length > length ? value.slice(0, length) + '...' : value;
}

function showError(message) {
    alert(message || 'Có lỗi xảy ra');
}

async function ensureAuth() {
    const response = await fetch(`${API_URL}/auth/me`);
    const result = await response.json();
    if (!result.success) {
        window.location.href = '/';
        return null;
    }
    return result.data;
}

function renderRows(rows) {
    currentLogs = rows || [];
    if (!rows.length) {
        DOM.etlJobLogPageBody.innerHTML = '<tr><td colspan="10" class="empty-state">Không có dữ liệu</td></tr>';
        return;
    }

    DOM.etlJobLogPageBody.innerHTML = rows.map(item => `
        <tr>
            <td>${item.id ?? '-'}</td>
            <td>${item.created_at || '-'}</td>
            <td>${item.job_name || '-'}</td>
            <td>${item.table_name || '-'}</td>
            <td>${item.operation_type || '-'}</td>
            <td>${item.error_level || '-'}</td>
            <td>
                <div class="log-message-preview" data-log-id="${item.id}" title="Click để xem full log">
                    ${truncate(item.message || '-', 150)}
                </div>
            </td>
            <td>${item.rows_inserted ?? '-'}</td>
            <td>${item.execution_time_ms ?? '-'}</td>
            <td>${item.created_by || '-'}</td>
        </tr>
    `).join('');
}

function closeFullLogModal() {
    DOM.fullLogModal?.classList.remove('show');
}

function openFullLogModalById(logId) {
    const item = (currentLogs || []).find(x => String(x.id) === String(logId));
    if (!item || !DOM.fullLogModal || !DOM.fullLogMeta || !DOM.fullLogContent) return;

    DOM.fullLogMeta.textContent = `ID=${item.id || '-'} | table=${item.table_name || '-'} | operation=${item.operation_type || '-'} | level=${item.error_level || '-'} | time=${item.created_at || '-'}`;
    const fullText = [
        `Message:\n${item.message || '-'}`,
        `\nError code: ${item.error_code || '-'}`,
        `\nSQL Path: ${item.sql_path || '-'}`,
        `\nTraceback:\n${item.error_traceback || '-'}`
    ].join('\n');
    DOM.fullLogContent.textContent = fullText;
    DOM.fullLogModal.classList.add('show');
}

function updatePagination(total = 0) {
    if (DOM.paginationInfo) {
        DOM.paginationInfo.textContent = total > 0
            ? `Trang ${currentPage} / ${totalPages} (tổng ${total})`
            : 'Trang 0';
    }
    if (DOM.btnPrevPage) DOM.btnPrevPage.disabled = currentPage <= 1;
    if (DOM.btnNextPage) DOM.btnNextPage.disabled = currentPage >= totalPages;
}

async function loadLogs() {
    const tableName = (DOM.tableSearchInput?.value || '').trim();
    const params = new URLSearchParams({
        page: String(currentPage),
        page_size: String(pageSize)
    });
    if (tableName) params.set('table_name', tableName);

    DOM.etlJobLogPageBody.innerHTML = '<tr><td colspan="10" class="loading">Đang tải...</td></tr>';

    try {
        const response = await fetch(`${API_URL}/etl-job-logs?${params.toString()}`);
        const result = await response.json();
        if (!result.success) {
            showError(result.error || 'Không tải được lịch sử ETL Job');
            return;
        }
        renderRows(result.data || []);
        const pg = result.pagination || {};
        totalPages = Math.max(1, Number(pg.total_pages || 1));
        updatePagination(Number(pg.total || 0));
    } catch (error) {
        showError('Lỗi kết nối: ' + error.message);
    }
}

async function doLogout() {
    await fetch(`${API_URL}/auth/logout`, { method: 'POST' });
    window.location.href = '/';
}

async function deleteLogsByDate() {
    const dateValue = prompt('Nhập ngày cần xóa log (YYYY-MM-DD):');
    if (!dateValue) return;
    const date = dateValue.trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
        showError('Ngày không đúng định dạng YYYY-MM-DD');
        return;
    }
    if (!confirm(`Bạn có chắc chắn muốn xóa lịch sử ngày ${date}?`)) return;

    try {
        const response = await fetch(`${API_URL}/etl-job-logs/delete-by-date`, {
            method: 'DELETE',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ date })
        });
        const result = await response.json();
        if (!result.success) {
            showError(result.error || 'Không thể xóa log theo ngày');
            return;
        }
        alert(result.message || 'Xóa log thành công');
        currentPage = 1;
        await loadLogs();
    } catch (error) {
        showError('Lỗi kết nối: ' + error.message);
    }
}

document.addEventListener('DOMContentLoaded', async () => {
    DOM.btnBackDashboard?.addEventListener('click', () => {
        window.location.href = '/';
    });

    DOM.btnLogout?.addEventListener('click', doLogout);
    DOM.btnDeleteLogsByDate?.addEventListener('click', deleteLogsByDate);

    DOM.btnRefreshLogs?.addEventListener('click', () => {
        currentPage = 1;
        loadLogs();
    });

    DOM.tableSearchInput?.addEventListener('input', (e) => {
        const value = (e.target.value || '').trim();
        if (DOM.btnClearTableSearch) {
            DOM.btnClearTableSearch.style.display = value ? 'block' : 'none';
        }
        clearTimeout(searchTimeout);
        searchTimeout = setTimeout(() => {
            currentPage = 1;
            loadLogs();
        }, 300);
    });

    DOM.btnClearTableSearch?.addEventListener('click', () => {
        if (!DOM.tableSearchInput) return;
        DOM.tableSearchInput.value = '';
        DOM.btnClearTableSearch.style.display = 'none';
        currentPage = 1;
        loadLogs();
    });

    DOM.pageSizeSelect?.addEventListener('change', (e) => {
        const next = parseInt(e.target.value, 10);
        pageSize = Number.isFinite(next) && next > 0 ? next : 50;
        currentPage = 1;
        loadLogs();
    });

    DOM.btnPrevPage?.addEventListener('click', () => {
        if (currentPage <= 1) return;
        currentPage -= 1;
        loadLogs();
    });

    DOM.btnNextPage?.addEventListener('click', () => {
        if (currentPage >= totalPages) return;
        currentPage += 1;
        loadLogs();
    });

    DOM.etlJobLogPageBody?.addEventListener('click', (e) => {
        const target = e.target.closest('.log-message-preview');
        if (!target) return;
        openFullLogModalById(target.dataset.logId);
    });

    const user = await ensureAuth();
    if (!user) return;
    const displayName = user.full_name || user.username;
    if (DOM.authUserInfo) {
        DOM.authUserInfo.style.display = '';
        DOM.authUserInfo.textContent = `${displayName} (${user.role})`;
    }
    if (DOM.btnDeleteLogsByDate) {
        DOM.btnDeleteLogsByDate.style.display = user.role === 'admin' ? '' : 'none';
    }
    if (DOM.pageSizeSelect) {
        DOM.pageSizeSelect.value = String(pageSize);
    }
    await loadLogs();
});
