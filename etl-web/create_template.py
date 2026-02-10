import pandas as pd

# Tạo file Excel template mẫu để import
template_data = {
    'job_type': ['1', '2', '3'],
    'schema_name': ['public', 'analytics', 'public'],
    'table_name': ['customers', 'sales_weekly', 'orders'],
    'sql_path': ['/sql/extract_customers.sql', '/sql/weekly_sales.sql', '/sql/monthly_orders.sql'],
    'description': ['Extract customer data', 'Weekly sales report', 'Monthly order summary'],
    'is_active': [True, True, False]
}

df = pd.DataFrame(template_data)

# Save to Excel
df.to_excel('template_import.xlsx', index=False, sheet_name='ETL_Jobs')

print("File template_import.xlsx da duoc tao thanh cong!")
print("\nCau truc file:")
print("- job_type: 1=Daily, 2=Weekly, 3=Month, 4=Quarter, 5=Year (BAT BUOC)")
print("- schema_name: Ten schema (BAT BUOC)")
print("- table_name: Ten bang (BAT BUOC)")
print("- sql_path: Duong dan file SQL (BAT BUOC)")
print("- description: Mo ta (tuy chon)")
print("- is_active: TRUE/FALSE (BAT BUOC)")
