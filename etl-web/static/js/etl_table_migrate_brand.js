const API_URL = '/api';

const DOM = {
    btnBackDashboard: document.getElementById('btnBackDashboard'),
    btnAddBrand: document.getElementById('btnAddBrand'),
    brandTableBody: document.getElementById('brandTableBody'),
    brandModal: document.getElementById('brandModal'),
    brandModalTitle: document.getElementById('brandModalTitle'),
    brandForm: document.getElementById('brandForm'),

    brandId: document.getElementById('brandId'),
    processGroupId: document.getElementById('processGroupId'),
    processGroupName: document.getElementById('processGroupName'),
    processId: document.getElementById('processId'),
    processName: document.getElementById('processName'),
    status: document.getElementById('status'),
    updatedAt: document.getElementById('updatedAt'),
    recordCount: document.getElementById('recordCount')
};

function showError(message) {
    alert(message || 'Có lỗi xảy ra');
}

function showSuccess(message) {
    alert(message || 'Thành công');
}

function closeBrandModal() {
    DOM.brandModal?.classList.remove('show');
    DOM.brandForm?.reset();
    if (DOM.brandId) DOM.brandId.value = '';
}

function openBrandModal({ title, row }) {
    DOM.brandModal?.classList.add('show');
    if (DOM.brandModalTitle) DOM.brandModalTitle.textContent = title || 'Thêm / Sửa';

    DOM.brandForm?.reset();
    if (DOM.brandId) DOM.brandId.value = '';

    if (row) {
        DOM.brandId.value = row.id ?? '';
        DOM.processGroupId.value = row.process_group_id ?? '';
        DOM.processGroupName.value = row.process_group_name ?? '';
        DOM.processId.value = row.process_id ?? '';
        DOM.processName.value = row.process_name ?? '';
        DOM.status.value = row.status ?? '';
        DOM.updatedAt.value = row.updated_at ?? '';
        DOM.recordCount.value = row.record_count ?? '';
    }
}

function parseNullableInt(value) {
    if (value === null || value === undefined) return null;
    const s = String(value).trim();
    if (!s) return null;
    return parseInt(s, 10);
}

function parseNullableString(value) {
    if (value === null || value === undefined) return null;
    const s = String(value).trim();
    return s ? s : null;
}

async function ensureAdmin() {
    const response = await fetch(`${API_URL}/auth/me`);
    const result = await response.json();
    if (!result.success) {
        window.location.href = '/';
        return null;
    }
    if (!result.data || result.data.role !== 'admin') {
        window.location.href = '/';
        return null;
    }
    return result.data;
}

function renderBrands(rows) {
    if (!rows || !rows.length) {
        DOM.brandTableBody.innerHTML = '<tr><td colspan="8" class="empty-state">Không có dữ liệu</td></tr>';
        return;
    }

    DOM.brandTableBody.innerHTML = rows.map(r => `
        <tr>
            <td>${r.id ?? '-'}</td>
            <td>${r.process_group_name ? r.process_group_name + ' (' + (r.process_group_id || '') + ')' : (r.process_group_id || '-')}</td>
            <td>${r.process_id ?? '-'}</td>
            <td>${r.process_name ?? '-'}</td>
            <td>${r.status ?? '-'}</td>
            <td>${r.updated_at ?? '-'}</td>
            <td>${r.record_count ?? '-'}</td>
            <td>
                <button class="btn btn-secondary btn-sm" title="Sửa" onclick="window.__editBrand(${r.id})">
                    <i class="fas fa-pen"></i>
                </button>
                <button class="btn btn-danger btn-sm brand-delete-btn" title="Xóa" onclick="window.__deleteBrand(${r.id})">
                    <i class="fas fa-trash"></i>
                </button>
            </td>
        </tr>
    `).join('');
}

async function loadBrands() {
    try {
        DOM.brandTableBody.innerHTML = '<tr><td colspan="8" class="loading">Đang tải...</td></tr>';
        const response = await fetch(`${API_URL}/etl-table-migrate-brand`);
        const result = await response.json();
        if (!result.success) {
            showError(result.error || 'Không tải được dữ liệu');
            return;
        }
        renderBrands(result.data || []);
    } catch (error) {
        showError('Lỗi kết nối: ' + error.message);
    }
}

async function createOrUpdateBrand(e) {
    e.preventDefault();

    const id = DOM.brandId?.value ? parseInt(DOM.brandId.value, 10) : null;

    const payload = {
        process_group_id: parseNullableString(DOM.processGroupId?.value),
        process_group_name: parseNullableString(DOM.processGroupName?.value),
        process_id: parseNullableString(DOM.processId?.value),
        process_name: parseNullableString(DOM.processName?.value),
        status: parseNullableInt(DOM.status?.value),
        updated_at: parseNullableString(DOM.updatedAt?.value),
        record_count: parseNullableInt(DOM.recordCount?.value)
    };

    const url = id ? `${API_URL}/etl-table-migrate-brand/${id}` : `${API_URL}/etl-table-migrate-brand`;
    const method = id ? 'PUT' : 'POST';

    try {
        const response = await fetch(url, {
            method,
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        const result = await response.json();
        if (!result.success) {
            showError(result.error || 'Lưu thất bại');
            return;
        }
        closeBrandModal();
        showSuccess(result.message || 'Đã lưu');
        await loadBrands();
    } catch (error) {
        showError('Lỗi kết nối: ' + error.message);
    }
}

async function deleteBrand(rowId) {
    if (!confirm('Bạn có chắc chắn muốn xóa bản ghi này?')) return;
    try {
        const response = await fetch(`${API_URL}/etl-table-migrate-brand/${rowId}`, { method: 'DELETE' });
        const result = await response.json();
        if (!result.success) {
            showError(result.error || 'Xóa thất bại');
            return;
        }
        showSuccess(result.message || 'Đã xóa');
        await loadBrands();
    } catch (error) {
        showError('Lỗi kết nối: ' + error.message);
    }
}

window.__editBrand = function (id) {
    // Load row via current table content is fine, but simplest is refetch.
    // For performance, we refetch list and find the row.
    (async () => {
        try {
            const response = await fetch(`${API_URL}/etl-table-migrate-brand`);
            const result = await response.json();
            if (!result.success) {
                showError(result.error || 'Không tải được dữ liệu');
                return;
            }
            const row = (result.data || []).find(x => String(x.id) === String(id));
            openBrandModal({ title: 'Chỉnh sửa ETL NIFI', row });
        } catch (error) {
            showError('Lỗi kết nối: ' + error.message);
        }
    })();
};

window.__deleteBrand = function (id) {
    deleteBrand(id);
};

document.addEventListener('DOMContentLoaded', async () => {
    if (DOM.btnBackDashboard) {
        DOM.btnBackDashboard.addEventListener('click', () => {
            window.location.href = '/';
        });
    }

    DOM.btnAddBrand?.addEventListener('click', () => {
        openBrandModal({ title: 'Thêm ETL NIFI', row: null });
    });

    DOM.brandForm?.addEventListener('submit', createOrUpdateBrand);

    const admin = await ensureAdmin();
    if (!admin) return;
    await loadBrands();
});

