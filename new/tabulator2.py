import pandas as pd
import requests
from io import StringIO

def fetch_csv_from_api(url_template, url_keys, headers=None):
    """Fetch CSV data from multiple API endpoints and combine into a single DataFrame."""
    dfs = []
    for key in url_keys:
        url = url_template.format(endpoint_key=key)
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            df = pd.read_csv(StringIO(response.text))
            dfs.append(df)
            print(f"Successfully fetched data from {url}")
        except requests.RequestException as e:
            print(f"Error fetching data from {url}: {e}")
        except pd.errors.ParserError as e:
            print(f"Error parsing CSV from {url}: {e}")
    if not dfs:
        print("Error: No data fetched from any endpoint.")
        return None
    # Combine all DataFrames, preserving all columns
    combined_df = pd.concat(dfs, ignore_index=True)
    return combined_df

def compare_dataframes(df_m7, df_ss, primary_key, ignore_pks, ignore_attrs):
    """Compare two DataFrames and generate comparison statistics for matching primary keys."""
    # Initialize results dictionary
    results = {
        'row_count_comparison': {},
        'mismatch_counts': {},
        'mismatch_samples': {},  # Dictionary to store samples per attribute
        'duplicate_pks': {'m7_duplicates': [], 'ss_duplicates': []}  # Store duplicate primary keys
    }

    # Ensure primary_key is a list
    primary_key = primary_key if isinstance(primary_key, list) else [primary_key]
    
    # Verify all primary key columns exist
    for pk in primary_key:
        if pk not in df_m7.columns or pk not in df_ss.columns:
            print(f"Error: Primary key column '{pk}' not found in one or both DataFrames.")
            return None

    # Filter out ignored primary key combinations
    if ignore_pks:
        # Convert ignore_pks to a DataFrame for filtering
        ignore_pks_df = pd.DataFrame(ignore_pks, columns=primary_key)
        df_m7 = df_m7.merge(ignore_pks_df, on=primary_key, how='left', indicator=True)
        df_m7 = df_m7[df_m7['_merge'] == 'left_only'].drop(columns=['_merge'])
        df_ss = df_ss.merge(ignore_pks_df, on=primary_key, how='left', indicator=True)
        df_ss = df_ss[df_ss['_merge'] == 'left_only'].drop(columns=['_merge'])

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

    # 1. Row count comparison
    results['row_count_comparison'] = {
        'm7_rows': len(df_m7),
        'ss_rows': len(df_ss),
        'match': len(df_m7) == len(df_ss)
    }

    # 2. Count mismatched values per attribute and collect 5 samples per attribute
    columns_to_compare = [col for col in df_m7.columns if col not in primary_key and col not in ignore_attrs]
    mismatch_counts = {col: 0 for col in columns_to_compare}
    mismatch_samples = {col: [] for col in columns_to_compare}
    max_samples_per_attr = 5

    # Iterate over rows using itertuples to avoid index-based access
    for row_m7, row_ss in zip(df_m7.itertuples(), df_ss.itertuples()):
        for col in columns_to_compare:
            val_m7 = getattr(row_m7, col)
            val_ss = getattr(row_ss, col)
            if str(val_m7) != str(val_ss) or (pd.isna(val_m7) != pd.isna(val_ss)):
                mismatch_counts[col] += 1
                if len(mismatch_samples[col]) < max_samples_per_attr:
                    pk_value = {pk: getattr(row_m7, pk) for pk in primary_key}
                    mismatch_samples[col].append({
                        'primary_key': pk_value,
                        'm7_value': val_m7,
                        'ss_value': val_ss
                    })

    results['mismatch_counts'] = mismatch_counts
    results['mismatch_samples'] = mismatch_samples

    return results

def print_report(results):
    """Print comparison report to console in a simple format."""
    if results is None:
        print("No report generated due to errors in comparison.")
        return

    print("=== DataFrame Comparison Report ===")

    # 0. Duplicate primary keys observation
    print("\n0. Duplicate Primary Keys:")
    print("-" * 30)
    print(f"{'DataFrame':<10} {'Duplicate Primary Keys':<20}")
    print("-" * 30)
    m7_dups = [str(d) for d in results['duplicate_pks']['m7_duplicates']]
    ss_dups = [str(d) for d in results['duplicate_pks']['ss_duplicates']]
    print(f"{'M7':<10} {', '.join(m7_dups) or 'None':<20}")
    print(f"{'SS':<10} {', '.join(ss_dups) or 'None':<20}")
    print("-" * 30)

    # 1. Row count comparison
    print("\n1. Row Count Comparison:")
    print("-" * 30)
    print(f"{'DataFrame':<10} {'Row Count':<10} {'Match':<10}")
    print("-" * 30)
    print(f"{'M7':<10} {results['row_count_comparison']['m7_rows']:<10} {'':<10}")
    print(f"{'SS':<10} {results['row_count_comparison']['ss_rows']:<10} "
          f"{'Yes' if results['row_count_comparison']['match'] else 'No':<10}")
    print("-" * 30)

    # 2. Mismatch counts by attribute (only show attributes with mismatches)
    print("\n2. Mismatch Counts by Attribute:")
    print("-" * 30)
    print(f"{'Attribute':<15} {'Mismatch Count':<15}")
    print("-" * 30)
    has_mismatches = False
    for attr, count in results['mismatch_counts'].items():
        if count > 0:  # Only print attributes with non-zero mismatches
            print(f"{attr:<15} {count:<15}")
            has_mismatches = True
    if not has_mismatches:
        print("No attributes with mismatches found.")
    print("-" * 30)

    # 3. Sample mismatches per attribute
    print("\n3. Sample Mismatches (up to 5 per attribute):")
    print("-" * 30)
    for attr, samples in results['mismatch_samples'].items():
        if samples:
            print(f"\nAttribute: {attr}")
            for i, sample in enumerate(samples, 1):
                print(f"Mismatch {i}:")
                pk_str = ', '.join(f"{k}={v}" for k, v in sample['primary_key'].items())
                print(f"M7: {pk_str} | {attr} = {sample['m7_value']}")
                print(f"SS: {pk_str} | {attr} = {sample['ss_value']}")
                print()
        else:
            print(f"\nAttribute: {attr}")
            print("No mismatches found.")
            print()

# Example usage
if __name__ == "__main__":
    # Sample API URLs and keys
    m7_url = "https://api.example.com/{endpoint_key}/m7.csv"
    m7_url_keys = ['A1', 'A2']
    ss_url = "https://api.example.com/{endpoint_key}/ss.csv"
    ss_url_keys = ['A1', 'A2']

    # Sample DataFrames for testing
    csv_m7_a1 = """id,dept_id,loc_id,name,age,city,notes
1,10,X,Alice,25,New York,Note1
1,10,Y,Bob,30,Los Angeles,Note2
2,20,X,Charlie,35,Chicago,Note3
3,30,Y,David,40,Houston,Note4
"""
    csv_m7_a2 = """id,dept_id,loc_id,name,age,city,notes
4,40,Z,Eve,45,Seattle,Note5
5,50,X,Frank,50,Denver,Note6
"""
    csv_ss_a1 = """id,dept_id,loc_id,name,age,city,notes
1,10,X,Alice,26,New York,Note1
1,10,Y,Bob,30,Los Angeles,Note2
2,20,X,Charlie,36,Chicagos,Note3
"""
    csv_ss_a2 = """id,dept_id,loc_id,name,age,city,notes
3,30,Y,David,41,Houston,Note4
4,40,Z,Eve,46,Seattles,Note5
"""

    # Simulate API responses for testing
    df_m7_a1 = pd.read_csv(StringIO(csv_m7_a1))
    df_m7_a2 = pd.read_csv(StringIO(csv_m7_a2))
    df_ss_a1 = pd.read_csv(StringIO(csv_ss_a1))
    df_ss_a2 = pd.read_csv(StringIO(csv_ss_a2))
    df_m7 = pd.concat([df_m7_a1, df_m7_a2], ignore_index=True)
    df_ss = pd.concat([df_ss_a1, df_ss_a2], ignore_index=True)

    # Uncomment to fetch from real APIs
    # df_m7 = fetch_csv_from_api(m7_url, m7_url_keys)
    # df_ss = fetch_csv_from_api(ss_url, ss_url_keys)

    # Check if DataFrames were loaded successfully
    if df_m7 is None or df_ss is None:
        print("Failed to load one or both DataFrames.")
    else:
        # Define primary key and ignore lists
        primary_key = ['id', 'dept_id', 'loc_id']
        ignore_pks = [{'id': 1, 'dept_id': 10, 'loc_id': 'Y'}]  # Ignore specific composite key
        ignore_attrs = ['notes']

        # Compare DataFrames
        results = compare_dataframes(df_m7, df_ss, primary_key, ignore_pks, ignore_attrs)

        # Print report
        print_report(results)
