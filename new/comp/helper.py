import pandas as pd
import requests
from io import StringIO

def fetch_csv_from_api(url_template, url_keys, primary_key, headers=None):
    """Fetch CSV data from multiple API endpoints and combine into a single DataFrame.
    Returns the combined DataFrame and a dictionary of row counts per url_key.
    """
    dfs = []
    row_counts = {}  # Track row counts per url_key
    # Ensure primary_key is a list
    primary_key = primary_key if isinstance(primary_key, list) else [primary_key]
    # Force primary key columns to be read as strings
    dtype = {pk: str for pk in primary_key}
    for key in url_keys:
        url = url_template.format(endpoint_key=key)
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            df = pd.read_csv(StringIO(response.text), dtype=dtype)
            row_counts[key] = len(df)
            dfs.append(df)
            print(f"Successfully fetched data from {url} ({row_counts[key]} rows)")
        except requests.RequestException as e:
            print(f"Error fetching data from {url}: {e}")
            row_counts[key] = 0
        except pd.errors.ParserError as e:
            print(f"Error parsing CSV from {url}: {e}")
            row_counts[key] = 0
    if not dfs:
        print("Error: No data fetched from any endpoint.")
        return None, row_counts
    # Combine all DataFrames, preserving all columns
    combined_df = pd.concat(dfs, ignore_index=True)
    return combined_df, row_counts

def compare_dataframes(df_m7, df_ss, primary_key, ignore_pks, ignore_attrs):
    """Compare two DataFrames and generate comparison statistics for matching primary keys."""
    # Initialize results dictionary
    results = {
        'row_count_comparison': {},
        'mismatch_counts': {},
        'mismatch_samples': {},  # Store unique mismatches with counts and one example
        'duplicate_pks': {'m7_duplicates': [], 'ss_duplicates': []},
        'unique_pks': {'m7_only': [], 'ss_only': []}  # Store unique keys not in the other DataFrame
    }

    # Ensure primary_key is a list
    primary_key = primary_key if isinstance(primary_key, list) else [primary_key]
    
    # Verify all primary key columns exist
    for pk in primary_key:
        if pk not in df_m7.columns or pk not in df_ss.columns:
            print(f"Error: Primary key column '{pk}' not found in one or both DataFrames.")
            return None

    # Convert primary key columns to strings (ensure consistency)
    for pk in primary_key:
        df_m7[pk] = df_m7[pk].astype(str)
        df_ss[pk] = df_ss[pk].astype(str)

    # Debug: Print data types for primary key columns
    print("Debug: Primary key column types in df_m7:", {pk: df_m7[pk].dtype for pk in primary_key})
    print("Debug: Primary key column types in df_ss:", {pk: df_ss[pk].dtype for pk in primary_key})

    # Find unique primary keys not present in the other DataFrame
    m7_pks = set(tuple(row) for row in df_m7[primary_key].values)
    ss_pks = set(tuple(row) for row in df_ss[primary_key].values)
    m7_only = m7_pks - ss_pks
    ss_only = ss_pks - m7_pks
    results['unique_pks']['m7_only'] = [dict(zip(primary_key, pk)) for pk in m7_only]
    results['unique_pks']['ss_only'] = [dict(zip(primary_key, pk)) for pk in ss_only]

    # Handle empty or None ignore_pks
    if not ignore_pks:  # Covers [], None, or other falsy values
        ignore_pks = []
    else:
        # Validate ignore_pks structure
        for pk_dict in ignore_pks:
            if not isinstance(pk_dict, dict) or any(pk not in pk_dict for pk in primary_key):
                print(f"Error: Invalid ignore_pks entry: {pk_dict}. Must be a dictionary with keys {primary_key}.")
                return None
        # Convert ignore_pks to a list of tuples for filtering
        ignore_pks_tuples = [tuple(str(pk_dict[pk]) for pk in primary_key) for pk_dict in ignore_pks]
        # Create a tuple of primary key values for each row
        m7_pk_tuples = [tuple(row) for row in df_m7[primary_key].values]
        ss_pk_tuples = [tuple(row) for row in df_ss[primary_key].values]
        # Filter out rows where primary keys match ignore_pks
        df_m7 = df_m7[~pd.Series(m7_pk_tuples).isin(ignore_pks_tuples)].copy()
        df_ss = df_ss[~pd.Series(ss_pk_tuples).isin(ignore_pks_tuples)].copy()

    # Detect duplicate primary key combinations
    m7_duplicates = df_m7[df_m7.duplicated(subset=primary_key, keep=False)][primary_key].to_dict('records')
    ss_duplicates = df_ss[df_ss.duplicated(subset=primary_key, keep=False)][primary_key].to_dict('records')
    results['duplicate_pks']['m7_duplicates'] = m7_duplicates
    results['duplicate_pks']['ss_duplicates'] = ss_duplicates

    # Filter out duplicate primary keys to keep only the first occurrence
    df_m7 = df_m7.drop_duplicates(subset=primary_key, keep='first').copy()
    df_ss = df_ss.drop_duplicates(subset=primary_key, keep='first').copy()

    # Perform inner join on primary key(s) to get matching rows
    df_m7 = df_m7.set_index(primary_key)
    df_ss = df_ss.set_index(primary_key)
    common_pks = df_m7.index.intersection(df_ss.index)
    if not common_pks.size:
        print("Error: No common primary keys found between DataFrames after filtering.")
        return None

    # Filter to common primary keys
    df_m7 = df_m7.loc[common_pks].reset_index()
    df_ss = df_ss.loc[common_pks].reset_index()

    # 1. Row count comparison (after filtering)
    results['row_count_comparison'] = {
        'm7_rows': len(df_m7),
        'ss_rows': len(df_ss),
        'match': len(df_m7) == len(df_ss)
    }

    # 2. Count mismatched values and collect unique mismatches with counts
    columns_to_compare = [col for col in df_m7.columns if col not in primary_key and col not in ignore_attrs]
    mismatch_counts = {col: 0 for col in columns_to_compare}
    mismatch_samples = {col: {} for col in columns_to_compare}  # Store { (m7_val, ss_val): {count, example} }

    # Iterate over rows using itertuples
    for row_m7, row_ss in zip(df_m7.itertuples(), df_ss.itertuples()):
        for col in columns_to_compare:
            val_m7 = getattr(row_m7, col)
            val_ss = getattr(row_ss, col)
            if str(val_m7) != str(val_ss) or (pd.isna(val_m7) != pd.isna(val_ss)):
                mismatch_counts[col] += 1
                # Use string representation for comparison to handle NaN consistently
                val_pair = (str(val_m7) if not pd.isna(val_m7) else 'NaN', 
                           str(val_ss) if not pd.isna(val_ss) else 'NaN')
                pk_value = {pk: getattr(row_m7, pk) for pk in primary_key}
                if val_pair not in mismatch_samples[col]:
                    mismatch_samples[col][val_pair] = {'count': 0, 'example': pk_value}
                mismatch_samples[col][val_pair]['count'] += 1

    results['mismatch_counts'] = mismatch_counts
    results['mismatch_samples'] = mismatch_samples

    return results

def print_report(results, m7_row_counts, ss_row_counts):
    """Print comparison report to console in a simple format."""
    if results is None:
        print("No report generated due to errors in comparison.")
        return

    print("=== DataFrame Comparison Report ===")

    # 0. Total rows fetched by API
    print("\n0. Total Rows Fetched by API:")
    print("-" * 30)
    print(f"{'DataFrame':<10} {'Total Rows':<10}")
    print("-" * 30)
    print(f"{'M7':<10} {sum(m7_row_counts.values()):<10}")
    print(f"{'SS':<10} {sum(ss_row_counts.values()):<10}")
    print("-" * 30)

    # 1. Row counts by URL key
    print("\n1. Row Counts by URL Key:")
    print("-" * 30)
    print(f"{'DataFrame':<10} {'URL Key':<15} {'Row Count':<10}")
    print("-" * 30)
    for key, count in m7_row_counts.items():
        print(f"{'M7':<10} {key:<15} {count:<10}")
    for key, count in ss_row_counts.items():
        print(f"{'SS':<10} {key:<15} {count:<10}")
    print("-" * 30)

    # 2. Unique primary keys not in the other DataFrame
    print("\n2. Unique Primary Keys Not in Other DataFrame:")
    print("-" * 30)
    print(f"{'DataFrame':<10} {'Unique Primary Keys':<30}")
    print("-" * 30)
    m7_unique = [str(d) for d in results['unique_pks']['m7_only']]
    ss_unique = [str(d) for d in results['unique_pks']['ss_only']]
    print(f"{'M7':<10} {', '.join(m7_unique) or 'None':<30}")
    print(f"{'SS':<10} {', '.join(ss_unique) or 'None':<30}")
    print("-" * 30)

    # 3. Duplicate primary keys
    print("\n3. Duplicate Primary Keys:")
    print("-" * 30)
    print(f"{'DataFrame':<10} {'Duplicate Primary Keys':<30}")
    print("-" * 30)
    m7_dups = [str(d) for d in results['duplicate_pks']['m7_duplicates']]
    ss_dups = [str(d) for d in results['duplicate_pks']['ss_duplicates']]
    print(f"{'M7':<10} {', '.join(m7_dups) or 'None':<30}")
    print(f"{'SS':<10} {', '.join(ss_dups) or 'None':<30}")
    print("-" * 30)

    # 4. Row count comparison (after filtering)
    print("\n4. Row Count Comparison (After Filtering):")
    print("-" * 30)
    print(f"{'DataFrame':<10} {'Row Count':<10} {'Match':<10}")
    print("-" * 30)
    print(f"{'M7':<10} {results['row_count_comparison']['m7_rows']:<10} {'':<10}")
    print(f"{'SS':<10} {results['row_count_comparison']['ss_rows']:<10} "
          f"{'Yes' if results['row_count_comparison']['match'] else 'No':<10}")
    print("-" * 30)

    # 5. Mismatch counts by attribute
    print("\n5. Mismatch Counts by Attribute:")
    print("-" * 30)
    print(f"{'Attribute':<15} {'Mismatch Count':<15}")
    print("-" * 30)
    has_mismatches = False
    for attr, count in results['mismatch_counts'].items():
        if count > 0:
            print(f"{attr:<15} {count:<15}")
            has_mismatches = True
    if not has_mismatches:
        print("No attributes with mismatches found.")
    print("-" * 30)

    # 6. Sample mismatches per attribute
    print("\n6. Sample Mismatches (unique mismatches with counts):")
    print("-" * 30)
    for attr, samples in results['mismatch_samples'].items():
        print(f"\nAttribute: {attr}")
        if samples:
            for i, ((m7_val, ss_val), info) in enumerate(samples.items(), 1):
                count = info['count']
                pk_value = info['example']
                print(f"Mismatch {i}: ({count} keys have similar mismatch)")
                pk_str = ', '.join(f"{k}={v}" for k, v in pk_value.items())
                print(f"M7: {pk_str} | {attr} = {m7_val if m7_val != 'NaN' else pd.NA}")
                print(f"SS: {pk_str} | {attr} = {ss_val if ss_val != 'NaN' else pd.NA}")
                print()
        else:
            print("No mismatches found.")
            print()

def write_html_report(results, m7_row_counts, ss_row_counts, output_file):
    """Write comparison report to an HTML file for better readability."""
    if results is None:
        with open(output_file, 'w') as f:
            f.write("<html><body><h1>DataFrame Comparison Report</h1><p>No report generated due to errors in comparison.</p></body></html>")
        return

    html_content = """
    <html>
    <head>
        <title>DataFrame Comparison Report</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            h1, h2 { color: #333; }
            table { border-collapse: collapse; width: 80%; margin-bottom: 20px; }
            th, td { border: 1px solid #ccc; padding: 8px; text-align: left; }
            th { background-color: #f2f2f2; }
            .section { margin-bottom: 30px; }
            .mismatch { margin-left: 20px; }
        </style>
    </head>
    <body>
        <h1>DataFrame Comparison Report</h1>
    """

    # 0. Total rows fetched by API
    html_content += """
        <div class='section'>
        <h2>0. Total Rows Fetched by API</h2>
        <table>
            <tr><th>DataFrame</th><th>Total Rows</th></tr>
    """
    html_content += f"""
        <tr><td>M7</td><td>{sum(m7_row_counts.values())}</td></tr>
        <tr><td>SS</td><td>{sum(ss_row_counts.values())}</td></tr>
    """
    html_content += "</table></div>"

    # 1. Row counts by URL key
    html_content += """
        <div class='section'>
        <h2>1. Row Counts by URL Key</h2>
        <table>
            <tr><th>DataFrame</th><th>URL Key</th><th>Row Count</th></tr>
    """
    for key, count in m7_row_counts.items():
        html_content += f"<tr><td>M7</td><td>{key}</td><td>{count}</td></tr>"
    for key, count in ss_row_counts.items():
        html_content += f"<tr><td>SS</td><td>{key}</td><td>{count}</td></tr>"
    html_content += "</table></div>"

    # 2. Unique primary keys not in the other DataFrame
    html_content += """
        <div class='section'>
        <h2>2. Unique Primary Keys Not in Other DataFrame</h2>
        <table>
            <tr><th>DataFrame</th><th>Unique Primary Keys</th></tr>
    """
    m7_unique = [str(d) for d in results['unique_pks']['m7_only']]
    ss_unique = [str(d) for d in results['unique_pks']['ss_only']]
    html_content += f"""
        <tr><td>M7</td><td>{', '.join(m7_unique) or 'None'}</td></tr>
        <tr><td>SS</td><td>{', '.join(ss_unique) or 'None'}</td></tr>
    """
    html_content += "</table></div>"

    # 3. Duplicate primary keys
    html_content += """
        <div class='section'>
        <h2>3. Duplicate Primary Keys</h2>
        <table>
            <tr><th>DataFrame</th><th>Duplicate Primary Keys</th></tr>
    """
    m7_dups = [str(d) for d in results['duplicate_pks']['m7_duplicates']]
    ss_dups = [str(d) for d in results['duplicate_pks']['ss_duplicates']]
    html_content += f"""
        <tr><td>M7</td><td>{', '.join(m7_dups) or 'None'}</td></tr>
        <tr><td>SS</td><td>{', '.join(ss_dups) or 'None'}</td></tr>
    """
    html_content += "</table></div>"

    # 4. Row count comparison (after filtering)
    html_content += """
        <div class='section'>
        <h2>4. Row Count Comparison (After Filtering)</h2>
        <table>
            <tr><th>DataFrame</th><th>Row Count</th><th>Match</th></tr>
    """
    html_content += f"""
        <tr><td>M7</td><td>{results['row_count_comparison']['m7_rows']}</td><td></td></tr>
        <tr><td>SS</td><td>{results['row_count_comparison']['ss_rows']}</td>
            <td>{'Yes' if results['row_count_comparison']['match'] else 'No'}</td></tr>
    """
    html_content += "</table></div>"

    # 5. Mismatch counts by attribute
    html_content += """
        <div class='section'>
        <h2>5. Mismatch Counts by Attribute</h2>
        <table>
            <tr><th>Attribute</th><th>Mismatch Count</th></tr>
    """
    has_mismatches = False
    for attr, count in results['mismatch_counts'].items():
        if count > 0:
            html_content += f"<tr><td>{attr}</td><td>{count}</td></tr>"
            has_mismatches = True
    if not has_mismatches:
        html_content += "<tr><td colspan='2'>No attributes with mismatches found.</td></tr>"
    html_content += "</table></div>"

    # 6. Sample mismatches per attribute
    html_content += """
        <div class='section'>
        <h2>6. Sample Mismatches (unique mismatches with counts)</h2>
    """
    for attr, samples in results['mismatch_samples'].items():
        html_content += f"<h3>Attribute: {attr}</h3>"
        if samples:
            for i, ((m7_val, ss_val), info) in enumerate(samples.items(), 1):
                count = info['count']
                pk_value = info['example']
                pk_str = ', '.join(f"{k}={v}" for k, v in pk_value.items())
                m7_display = m7_val if m7_val != 'NaN' else 'NULL'
                ss_display = ss_val if ss_val != 'NaN' else 'NULL'
                html_content += f"""
                    <div class='mismatch'>
                    <p><strong>Mismatch {i}: ({count} keys have similar mismatch)</strong></p>
                    <p>M7: {pk_str} | {attr} = {m7_display}</p>
                    <p>SS: {pk_str} | {attr} = {ss_display}</p>
                    </div>
                """
        else:
            html_content += "<p>No mismatches found.</p>"
    html_content += "</div>"

    html_content += "</body></html>"

    # Write to file
    with open(output_file, 'w') as f:
        f.write(html_content)
    print(f"Report written to {output_file}")
