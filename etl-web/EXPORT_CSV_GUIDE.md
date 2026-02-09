# Hướng dẫn Export CSV

Guide chi tiết về tính năng Export dữ liệu ra file CSV.

## 📋 Tổng quan

Tính năng Export CSV cho phép bạn:
- ✅ Export tất cả dữ liệu
- ✅ Export dữ liệu đã filter
- ✅ Export dữ liệu đã search
- ✅ File CSV tương thích với Excel
- ✅ Encoding UTF-8 with BOM (hiển thị tiếng Việt đúng)

## 🚀 Cách sử dụng

### Export tất cả dữ liệu

1. Không chọn filter nào (All)
2. Click button **"Export CSV"**
3. File CSV tự động download

### Export dữ liệu đã filter

1. Chọn filter:
   - Loại job (Daily, Weekly, Month, ...)
   - Trạng thái (Active/Inactive)
   - Search theo tên bảng
2. Click **"Export CSV"**
3. Chỉ dữ liệu đang hiển thị được export

## 📊 Cấu trúc File CSV

### Columns (9 cột)

| Cột | Nội dung | Ví dụ |
|-----|----------|-------|
| ID | ID bản ghi | 1, 2, 3, ... |
| Loại Job | Loại + Label | 1 - Daily, 2 - Weekly |
| Schema | Schema name | public, analytics |
| Tên Bảng | Table name | customers, orders |
| Đường dẫn SQL | SQL path | /sql/extract.sql |
| Mô tả | Description | Extract customer data |
| Trạng thái | Active/Inactive | Hoạt động, Không hoạt động |
| Ngày tạo | Created timestamp | 2026-02-09T10:30:00 |
| Ngày cập nhật | Updated timestamp | 2026-02-09T11:45:00 |

### Example CSV Content

```csv
ID,Loại Job,Schema,Tên Bảng,Đường dẫn SQL,Mô tả,Trạng thái,Ngày tạo,Ngày cập nhật
1,"1 - Daily","public","customers","/sql/extract.sql","Extract data","Hoạt động","2026-02-09T10:30:00","2026-02-09T10:30:00"
2,"2 - Weekly","analytics","sales","/sql/weekly.sql","Weekly report","Hoạt động","2026-02-09T11:00:00","2026-02-09T11:00:00"
```

## 🔧 Technical Details

### Frontend Implementation

```javascript
function exportToCSV() {
    // 1. Get current filtered data
    let dataToExport = allJobs;
    
    // 2. Apply filters
    // - Job type filter
    // - Status filter
    // - Search filter
    
    // 3. Create CSV content
    // - Headers
    // - Rows with quoted values
    
    // 4. Add BOM for UTF-8 (Excel compatibility)
    const BOM = '\uFEFF';
    
    // 5. Create Blob and download
    const blob = new Blob([BOM + csvContent], { 
        type: 'text/csv;charset=utf-8;' 
    });
    
    // 6. Generate filename with timestamp
    const filename = `etl_jobs_${timestamp}.csv`;
    
    // 7. Trigger download
}
```

### Filename Format

```
etl_jobs_YYYY-MM-DDTHH-mm-ss.csv

Example:
etl_jobs_2026-02-09T16-30-45.csv
```

### UTF-8 BOM

File CSV bắt đầu với BOM (Byte Order Mark) `\uFEFF`:
- ✅ Excel mở được tiếng Việt đúng
- ✅ Không cần import với encoding manual
- ✅ Compatible với Windows Excel

## 📝 Use Cases

### 1. Export tất cả
```
Filter: All
Status: All
Search: (empty)
→ Export tất cả jobs
```

### 2. Export chỉ Daily jobs
```
Filter: Daily (1)
Status: All
Search: (empty)
→ Export chỉ jobs loại Daily
```

### 3. Export Active jobs
```
Filter: All
Status: Active
Search: (empty)
→ Export chỉ jobs đang hoạt động
```

### 4. Export search results
```
Filter: All
Status: All
Search: "customer"
→ Export chỉ jobs có table_name chứa "customer"
```

### 5. Export combined filters
```
Filter: Weekly (2)
Status: Active
Search: "sales"
→ Export Weekly jobs, Active, có "sales" trong tên
```

## 💡 Features

### ✅ Đã implement

- ✅ Export all data
- ✅ Export filtered data
- ✅ UTF-8 BOM encoding
- ✅ Auto filename with timestamp
- ✅ Excel compatible
- ✅ Quoted CSV values
- ✅ Success message
- ✅ Empty data validation

### 🚫 Không có

- Export to Excel (.xlsx)
- Custom column selection
- Export templates
- Scheduled exports

## 🎯 CSV Format Details

### Header Row
```csv
ID,Loại Job,Schema,Tên Bảng,Đường dẫn SQL,Mô tả,Trạng thái,Ngày tạo,Ngày cập nhật
```

### Data Rows
- All values quoted with `"`
- Commas inside values are safe (because quoted)
- Special characters preserved
- Encoding: UTF-8 with BOM

### Example with Special Characters

```csv
"1","1 - Daily","public","customers, orders","/sql/extract.sql","Extract ""customer"" data","Hoạt động","2026-02-09T10:30:00","2026-02-09T10:30:00"
```

Notes:
- Comma in table_name: `"customers, orders"` (quoted)
- Quote in description: `Extract ""customer"" data` (escaped)

## 🔍 Testing

### Test 1: Export All
1. Không chọn filter
2. Click "Export CSV"
3. Verify: File download với tất cả records

### Test 2: Export Filtered
1. Chọn filter "Daily"
2. Click "Export CSV"
3. Verify: File chỉ chứa Daily jobs

### Test 3: Export Empty
1. Search "xyz123" (không có result)
2. Click "Export CSV"
3. Verify: Error message "Không có dữ liệu để export!"

### Test 4: Excel Compatibility
1. Export file
2. Mở bằng Excel
3. Verify:
   - Tiếng Việt hiển thị đúng
   - Columns tách đúng
   - Special characters OK

### Test 5: Filename
1. Export file
2. Verify filename format:
   - `etl_jobs_YYYY-MM-DDTHH-mm-ss.csv`
   - Timestamp correct

## 📊 Performance

### Small Dataset (< 100 rows)
- Generation: < 100ms
- Download: Instant

### Medium Dataset (100-1000 rows)
- Generation: 100-500ms
- Download: < 1s

### Large Dataset (1000-5000 rows)
- Generation: 500ms-2s
- Download: 1-3s

## 💾 File Size

### Estimates
- 100 rows: ~15-20 KB
- 1000 rows: ~150-200 KB
- 5000 rows: ~750 KB - 1 MB

## 🐛 Troubleshooting

### Issue: Tiếng Việt bị lỗi trong Excel

**Solution**: File đã có BOM, nên OK. Nếu vẫn lỗi:
1. Mở file bằng Notepad
2. Save As → Encoding: UTF-8 with BOM

### Issue: Columns không tách đúng

**Solution**: 
- Check delimiter là comma (`,`)
- Excel → Data → From Text → Delimiter: Comma

### Issue: File không download

**Solution**:
- Check browser popup blocker
- Check browser download settings
- Try different browser

### Issue: "Không có dữ liệu để export"

**Solution**:
- Check filters (có thể filter hết data)
- Clear filters và thử lại
- Verify có data trong database

## 📚 Related

- [README.md](README.md) - Project overview
- [IMPORT_EXCEL_GUIDE.md](IMPORT_EXCEL_GUIDE.md) - Import guide
- [USAGE.md](USAGE.md) - User guide

## 💡 Tips

### 1. Export trước khi Import
- Backup data bằng cách export CSV
- Import Excel mới
- So sánh nếu cần

### 2. Regular Backups
- Export CSV định kỳ
- Lưu trữ cho audit trail
- Recovery nếu cần

### 3. Filter để export subset
- Export chỉ data cần thiết
- Smaller file size
- Faster processing

### 4. Use in Excel
- Mở file CSV trong Excel
- Sort, filter, analyze
- Create charts/pivots

## 🔐 Security Notes

- ✅ Export chỉ data user có quyền xem
- ✅ No sensitive data in filename
- ✅ Client-side generation (no server upload)
- ✅ Temporary blob URL (auto cleanup)

## 📈 Future Enhancements

1. **Export to Excel (.xlsx)** - Full Excel format
2. **Custom columns** - Choose which columns to export
3. **Export templates** - Pre-configured exports
4. **Scheduled exports** - Auto export daily/weekly
5. **Email export** - Send CSV via email
6. **Export history** - Track exported files
7. **Compression** - ZIP large files
8. **Multiple formats** - JSON, XML, etc.

---

**Version**: 1.0  
**Last Updated**: 2026-02-09  
**Feature**: CSV Export with UTF-8 BOM
