
import json
import csv
import pyodbc
from datetime import datetime
import os

def safe_value(val):
    """Return empty string for null/empty values (CSV compatible)"""
    if val is None or val == '' or val == 'null':
        return ''
    
    # Convert to string and sanitize for CSV
    val_str = str(val).strip()
    
    # Remove problematic characters
    # Remove newlines and carriage returns
    val_str = val_str.replace('\n', ' ').replace('\r', ' ')
    
    # Remove extra whitespace
    val_str = ' '.join(val_str.split())
    
    # Handle special characters that might break CSV
    # Remove or replace pipe characters if used as delimiter elsewhere
    val_str = val_str.replace('|', '-')
    
    return val_str

def parse_date(date_str):
    """Convert date from DD/MM/YYYY to YYYY-MM-DD format"""
    if not date_str or date_str == 'null':
        return None
    try:
        return datetime.strptime(date_str, '%d/%m/%Y').strftime('%Y-%m-%d')
    except:
        return None

def parse_date_ddmmyyyy(date_str):
    """Convert date from DDMMYYYY to YYYY-MM-DD format"""
    if not date_str or date_str == 'null' or len(date_str) != 8:
        return None
    try:
        return f"{date_str[4:8]}-{date_str[2:4]}-{date_str[0:2]}"
    except:
        return None

def flatten_segment_to_fields(segment_data, parse_dates=False):
    """
    Flatten segment data (dict, list, or nested structures) into sequential field values
    """
    fields = []
    
    if isinstance(segment_data, dict):
        for key, value in segment_data.items():
            if isinstance(value, dict):
                fields.extend(flatten_segment_to_fields(value, parse_dates))
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, (dict, list)):
                        fields.extend(flatten_segment_to_fields(item, parse_dates))
                    else:
                        fields.append(safe_value(item))
            else:
                fields.append(safe_value(value))
    elif isinstance(segment_data, list):
        for item in segment_data:
            if isinstance(item, (dict, list)):
                fields.extend(flatten_segment_to_fields(item, parse_dates))
            else:
                fields.append(safe_value(item))
    else:
        fields.append(safe_value(segment_data))
    
    return fields

def generate_pn_segment_row(unique_id, record, garbage_per_segment):
    """Generate single PN segment row with all data flattened sequentially"""
    pn = record.get('PN', {})
    
    row = [unique_id, 'PN']
    
    if pn:
        fields = flatten_segment_to_fields(pn)
        
        # Apply transformations to specific fields
        # TODO: Add your field transformations here
        # Example:
        # if len(fields) > 5 and fields[5]:  # If Field6 is a date in DD/MM/YYYY
        #     fields[5] = parse_date(fields[5])
        # if len(fields) > 10 and fields[10]:  # If Field11 is a date in DDMMYYYY
        #     fields[10] = parse_date_ddmmyyyy(fields[10])
        
        row.extend(fields)
    
    # Pad to 52 fields (before Residual_Value)
    while len(row) < 52:
        row.append('')
    
    # Add Residual_Value (garbage for PN segment)
    residual_value = safe_value(garbage_per_segment.get('PN')) if garbage_per_segment else ''
    row.append(residual_value)
    
    # Add new columns with empty values
    row.extend(['', '', '', '', '', ''])  # System, Processing_Stage, Processing_Status, Bot_Status, Failure_Reason, Forced_Status
    
    return row[:59]  # Changed from 53 to 59 (6 additional columns)

def generate_id_segment_row(unique_id, record, garbage_per_segment):
    """Generate single ID segment row with all IDs flattened sequentially"""
    ids = record.get('ID', [])
    
    row = [unique_id, 'ID']
    
    if ids:
        fields = flatten_segment_to_fields(ids)
        row.extend(fields)
    
    while len(row) < 52:
        row.append('')
    
    residual_value = safe_value(garbage_per_segment.get('ID')) if garbage_per_segment else ''
    row.append(residual_value)
    
    # Add new columns with empty values
    row.extend(['', '', '', '', '', ''])
    
    return row[:59]

def generate_pt_segment_row(unique_id, record, garbage_per_segment):
    """Generate single PT segment row with all phones flattened sequentially"""
    pts = record.get('PT', [])
    
    row = [unique_id, 'PT']
    
    if pts:
        fields = flatten_segment_to_fields(pts)
        row.extend(fields)
    
    while len(row) < 52:
        row.append('')
    
    residual_value = safe_value(garbage_per_segment.get('PT')) if garbage_per_segment else ''
    row.append(residual_value)
    
    # Add new columns with empty values
    row.extend(['', '', '', '', '', ''])
    
    return row[:59]

def generate_ec_segment_row(unique_id, record, garbage_per_segment):
    """Generate single EC segment row with all emails flattened sequentially"""
    ec = record.get('EC', {})
    
    row = [unique_id, 'EC']
    
    if ec:
        fields = flatten_segment_to_fields(ec)
        row.extend(fields)
    
    while len(row) < 52:
        row.append('')
    
    residual_value = safe_value(garbage_per_segment.get('EC')) if garbage_per_segment else ''
    row.append(residual_value)
    
    # Add new columns with empty values
    row.extend(['', '', '', '', '', ''])
    
    return row[:59]

def generate_pa_segment_row(unique_id, record, garbage_per_segment):
    """Generate single PA segment row with all addresses flattened sequentially"""
    pas = record.get('PA', [])
    
    row = [unique_id, 'PA']
    
    if pas:
        fields = flatten_segment_to_fields(pas)
        row.extend(fields)
    
    while len(row) < 52:
        row.append('')
    
    residual_value = safe_value(garbage_per_segment.get('PA')) if garbage_per_segment else ''
    row.append(residual_value)
    
    # Add new columns with empty values
    row.extend(['', '', '', '', '', ''])
    
    return row[:59]

def generate_tl_segment_row(unique_id, record, garbage_per_segment):
    """Generate single TL segment row with all data flattened sequentially"""
    tl = record.get('TL', {})
    
    row = [unique_id, 'TL']
    
    if tl:
        fields = flatten_segment_to_fields(tl)
        row.extend(fields)
    
    while len(row) < 52:
        row.append('')
    
    residual_value = safe_value(garbage_per_segment.get('TL')) if garbage_per_segment else ''
    row.append(residual_value)
    
    # Add new columns with empty values
    row.extend(['', '', '', '', '', ''])
    
    return row[:59]

def generate_th_segment_row(unique_id, record, garbage_per_segment):
    """Generate single TH segment row with all history records flattened sequentially"""
    ths = record.get('TH', [])
    
    row = [unique_id, 'TH']
    
    if ths:
        fields = flatten_segment_to_fields(ths)
        row.extend(fields)
    
    while len(row) < 52:
        row.append('')
    
    residual_value = safe_value(garbage_per_segment.get('TH')) if garbage_per_segment else ''
    row.append(residual_value)
    
    # Add new columns with empty values
    row.extend(['', '', '', '', '', ''])
    
    return row[:59]

def generate_all_segment_rows(unique_id, record, garbage_per_segment, debug=False):
    """Generate all segment rows (one row per segment) for a single record"""
    all_rows = []
    
    all_rows.append(generate_pn_segment_row(unique_id, record, garbage_per_segment))
    all_rows.append(generate_id_segment_row(unique_id, record, garbage_per_segment))
    all_rows.append(generate_pt_segment_row(unique_id, record, garbage_per_segment))
    all_rows.append(generate_ec_segment_row(unique_id, record, garbage_per_segment))
    all_rows.append(generate_pa_segment_row(unique_id, record, garbage_per_segment))
    all_rows.append(generate_tl_segment_row(unique_id, record, garbage_per_segment))
    all_rows.append(generate_th_segment_row(unique_id, record, garbage_per_segment))
    
    return all_rows

def generate_csv_from_json(json_file_path, csv_output_path, use_quotes=True):
    """Generate CSV file from JSON data - OPTIMIZED VERSION
    
    Args:
        use_quotes: If True, use QUOTE_ALL (safer but requires SQL Server 2017+)
                   If False, use QUOTE_MINIMAL (compatible with older SQL Server)
    """
    
    print("Loading JSON file...")
    with open(json_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    records = data.get('Records', [])
    total_records = len(records)
    print(f'Total records: {total_records}\n')
    
    # CSV Header - Updated with new columns
    header = ['Unique_ID', 'Segment_Name']
    header.extend([f'Field{i}' for i in range(1, 51)])
    header.extend(['Residual_Value', 'System', 'Processing_Stage', 'Processing_Status', 
                   'Bot_Status', 'Failure_Reason', 'Forced_Status'])
    
    print(f"Creating CSV file: {csv_output_path}")
    print(f"Quote mode: {'QUOTE_ALL (safer)' if use_quotes else 'QUOTE_MINIMAL (compatible)'}")
    print("Processing in batches for better performance...")
    
    # Write to CSV with proper escaping
    with open(csv_output_path, 'w', newline='', encoding='utf-8', buffering=8192*1024) as csvfile:
        # Choose quoting mode based on parameter
        csv_writer = csv.writer(csvfile, 
                               quoting=csv.QUOTE_ALL if use_quotes else csv.QUOTE_MINIMAL,
                               escapechar='\\',
                               doublequote=True)
        
        # Write header
        csv_writer.writerow(header)
        
        # Process in batches
        rows_written = 0
        problematic_rows = 0
        batch_size = 1000  # Process 1000 records at a time
        all_rows_batch = []
        
        for idx, record_obj in enumerate(records):
            unique_id = record_obj.get('unique_id')
            record = record_obj.get('record', {})
            garbage_per_segment = record_obj.get('garbage_per_segment', {})
            
            # Generate 7 rows (one per segment)
            segment_rows = generate_all_segment_rows(unique_id, record, garbage_per_segment)
            
            # Clean and add to batch
            for row in segment_rows:
                try:
                    # Additional validation: ensure all values are strings or empty
                    cleaned_row = []
                    for cell in row:
                        if cell == '':
                            cleaned_row.append('')
                        else:
                            # Convert and clean
                            cell_str = str(cell).strip()
                            # Remove any potential SQL injection or problematic patterns
                            if len(cell_str) > 8000:  # SQL Server varchar limit
                                cell_str = cell_str[:8000]
                                problematic_rows += 1
                            cleaned_row.append(cell_str)
                    
                    all_rows_batch.append(cleaned_row)
                    
                except Exception as e:
                    print(f"  Warning: Error processing row for {unique_id}: {str(e)}")
                    problematic_rows += 1
            
            # Write batch when it reaches batch_size records (7000 rows since 7 rows per record)
            if (idx + 1) % batch_size == 0:
                csv_writer.writerows(all_rows_batch)
                rows_written += len(all_rows_batch)
                all_rows_batch = []
                print(f"Processed {idx + 1}/{total_records} records... ({rows_written} rows written)")
        
        # Write remaining rows
        if all_rows_batch:
            csv_writer.writerows(all_rows_batch)
            rows_written += len(all_rows_batch)
            print(f"Processed {total_records}/{total_records} records... ({rows_written} rows written - FINAL)")
        
        print(f"\n✓ CSV file created successfully!")
        print(f"  Total records processed: {total_records}")
        print(f"  Total rows written: {rows_written}")
        print(f"  Expected rows: {total_records * 7}")
        if problematic_rows > 0:
            print(f"  ⚠ Warning: {problematic_rows} rows had data issues (truncated or errors)")
        print(f"  File location: {csv_output_path}")
        
    return rows_written

def execute_bulk_insert(connection_string, csv_network_path):
    """Execute BULK INSERT command via SQL Server"""
    
    print("\n" + "=" * 60)
    print("EXECUTING BULK INSERT")
    print("=" * 60)
    
    bulk_insert_query = f"""BULK INSERT [CIC_Reporting].[dbo].[Consumer_Main_Table]
    FROM '{csv_network_path}'
    WITH (
    FORMAT = 'CSV', 
    FIRSTROW = 2,
    DATAFILETYPE = 'char',
    TABLOCK
    );"""
    
    try:
        print(f"Connecting to SQL Server...")
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        # Check SQL Server version
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()[0]
        print(f"SQL Server Version: {version[:80]}...")
        
        print(f"\nExecuting BULK INSERT from: {csv_network_path}")
        print(f"Note: Max 10 errors allowed, will continue with valid rows")
        
        cursor.execute(bulk_insert_query)
        conn.commit()
        
        # Get row count to verify
        cursor.execute("SELECT COUNT(*) FROM [CIC_Reporting].[dbo].[Consumer_Main_Table]")
        row_count = cursor.fetchone()[0]
        
        print(f"\n✓ BULK INSERT completed successfully!")
        print(f"  Total rows in table: {row_count}")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"\n✗ BULK INSERT failed: {str(e)}")
        import traceback
        traceback.print_exc()
        
        # If quoted fields are causing issues, suggest alternative
        if "syntax" in str(e).lower() or "terminated" in str(e).lower():
            print("\n" + "!" * 60)
            print("TROUBLESHOOTING: If BULK INSERT continues to fail,")
            print("the issue might be with quoted CSV fields.")
            print("Try regenerating CSV with QUOTE_MINIMAL instead of QUOTE_ALL")
            print("!" * 60)
        
        return False

def main(json_file_path, connection_string, network_csv_path):
    """Main process: Generate CSV and execute BULK INSERT"""
    
    print("=" * 80)
    print("BULK INSERT PROCESS - CSV GENERATION AND INSERTION")
    print("=" * 80)
    print("Each record creates 7 rows (one per segment: PN, ID, PT, EC, PA, TL, TH)")
    print("Each row includes Residual_Value and 6 additional columns\n")
    
    # Step 1: Generate CSV file
    print("\nSTEP 1: Generating CSV file...")
    print("-" * 60)
    
    try:
        rows_written = generate_csv_from_json(json_file_path, network_csv_path)
        
        # Verify file exists
        if not os.path.exists(network_csv_path):
            raise FileNotFoundError(f"CSV file was not created at {network_csv_path}")
        
        file_size = os.path.getsize(network_csv_path) / (1024 * 1024)  # Size in MB
        print(f"  File size: {file_size:.2f} MB")
        
    except Exception as e:
        print(f"\n✗ CSV generation failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return
    
    # Step 2: Execute BULK INSERT
    print("\n\nSTEP 2: Executing BULK INSERT...")
    print("-" * 60)
    
    success = execute_bulk_insert(connection_string, network_csv_path)
    
    # Summary
    print("\n" + "=" * 80)
    if success:
        print("✓ PROCESS COMPLETED SUCCESSFULLY!")
    else:
        print("✗ PROCESS FAILED - CHECK ERRORS ABOVE")
    print("=" * 80)

# Usage
if __name__ == "__main__":
    connection_string = (
#.........your database connection ...........
    )
    
    json_file_path = r"C:\Users\admin\Desktop\CIC\TUDF_Output.json"
    network_csv_path = r"\c$\Bulk Insert\CIC\consumer_tudf_output.csv"
    
    main(json_file_path, connection_string, network_csv_path)
