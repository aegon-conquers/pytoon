import pandas as pd
import requests
from io import StringIO

def fetch_csv_from_api(url, headers=None):
    """Fetch CSV data from an API endpoint."""
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return pd.read_csv(StringIO(response.text))
    except requests.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return None
    except pd.errors.ParserError as e:
        print(f"Error parsing CSV from {url}: {e}")
        return None

def compare_dataframes(df_m7, df_ss, primary_key, ignore_pks, ignore_attrs):
    """Compare two DataFrames and generate comparison statistics for matching primary keys."""
    # Initialize results dictionary
    results = {
        'row_count_comparison': {},
        'mismatch_counts': {},
        'mismatch_samples': {}  # Dictionary to store samples per attribute
    }

    # Filter out ignored primary keys
    df_m7 = df_m7[~df_m7[primary_key].isin(ignore_pks)].copy()
    df_ss = df_ss[~df_ss[primary_key].isin(ignore_pks)].copy()

    # Verify primary key exists
    if primary_key not in df_m7.columns or primary_key not in df_ss.columns:
        print(f"Error: Primary key '{primary_key}' not found in one or both DataFrames.")
        return None

    # Perform inner join on primary key to get matching rows
    df_m7 = df_m7.set_index(primary_key)
    df_ss = df_ss.set_index(primary_key)
    common_pks = df_m7.index.intersection(df_ss.index)
    if not common_pks.size:
        print("Error: No common primary keys found between DataFrames after filtering.")
        return None

    # Filter to common primary keys
    df_m7 = df_m7.loc[common_pks].reset_index()
    df_ss = df_ss.loc[common_pks].reset_index()

    # Ensure primary keys are unique in both DataFrames
    if df_m7[primary_key].duplicated().any() or df_ss[primary_key].duplicated().any():
        print("Error: Duplicate primary keys found in one or both DataFrames.")
        return None

    # 1. Row count comparison
    results['row_count_comparison'] = {
        'm7_rows': len(df_m7),
        'ss_rows': len(df_ss),
        'match': len(df_m7) == len(df_ss)
    }

    # 2. Count mismatched values per attribute and collect 5 samples per attribute
    columns_to_compare = [col for col in df_m7.columns if col != primary_key and col not in ignore_attrs]
    mismatch_counts = {col: 0 for col in columns_to_compare}
    mismatch_samples = {col: [] for col in columns_to_compare}
    max_samples_per_attr = 5

    # Iterate over rows using itertuples to avoid iloc
    for row_m7, row_ss in zip(df_m7.itertuples(), df_ss.itertuples()):
        for col in columns_to_compare:
            val_m7 = getattr(row_m7, col)
            val_ss = getattr(row_ss, col)
            if str(val_m7) != str(val_ss) or (pd.isna(val_m7) != pd.isna(val_ss)):
                mismatch_counts[col] += 1
                if len(mismatch_samples[col]) < max_samples_per_attr:
                    pk_value = getattr(row_m7, primary_key)
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

    # 1. Row count comparison
    print("\n1. Row Count Comparison:")
    print("-" * 30)
    print(f"{'DataFrame':<10} {'Row Count':<10} {'Match':<10}")
    print("-" * 30)
    print(f"{'M7':<10} {results['row_count_comparison']['m7_rows']:<10} {'':<10}")
    print(f"{'SS':<10} {results['row_count_comparison']['ss_rows']:<10} "
          f"{'Yes' if results['row_count_comparison']['match'] else 'No':<10}")
    print("-" * 30)

    # 2. Mismatch counts by attribute
    print("\n2. Mismatch Counts by Attribute:")
    print("-" * 30)
    print(f"{'Attribute':<15} {'Mismatch Count':<15}")
    print("-" * 30)
    for attr, count in results['mismatch_counts'].items():
        print(f"{attr:<15} {count:<15}")
    print("-" * 30)

    # 3. Sample mismatches per attribute
    print("\n3. Sample Mismatches (up to 5 per attribute):")
    print("-" * 30)
    for attr, samples in results['mismatch_samples'].items():
        if samples:  # Only print if there are mismatches for this attribute
            print(f"\nAttribute: {attr}")
            for i, sample in enumerate(samples, 1):
                print(f"Mismatch {i}:")
                print(f"M7: {sample['primary_key']} | {attr} = {sample['m7_value']}")
                print(f"SS: {sample['primary_key']} | {attr} = {sample['ss_value']}")
                print()
        else:
            print(f"\nAttribute: {attr}")
            print("No mismatches found.")
            print()

# Example usage
if __name__ == "__main__":
    # Sample API URLs (replace with actual URLs)
    m7_url = "https://api.example.com/m7.csv"
    ss_url = "https://api.example.com/ss.csv"

    # Sample DataFrames for testing
    csv_m7 = """id,name,age,city,notes
1,Alice,25,New York,Note1
2,Bob,30,Los Angeles,Note2
3,Charlie,35,Chicago,Note3
4,David,40,Houston,Note4
5,Eve,45,Seattle,Note5
6,Frank,50,Denver,Note6
"""
    csv_ss = """id,name,age,city,notes
1,Alice,26,New York,Note1
2,Bob,30,Los Angeles,Note2
3,Charlie,36,Chicagos,Note3
4,David,41,Houston,Note4
5,Eve,46,Seattles,Note5
6,Frank,51,Denvers,Note6
"""

    # Simulate API responses for testing (comment out when using real APIs)
    df_m7 = pd.read_csv(StringIO(csv_m7))
    df_ss = pd.read_csv(StringIO(csv_ss))

    # Uncomment to fetch from real APIs
    # df_m7 = fetch_csv_from_api(m7_url)
    # df_ss = fetch_csv_from_api(ss_url)

    # Check if DataFrames were loaded successfully
    if df_m7 is None or df_ss is None:
        print("Failed to load one or both DataFrames.")
    else:
        # Define primary key and ignore lists
        primary_key = 'id'
        ignore_pks = [2]  # Ignore rows where id=2
        ignore_attrs = ['notes']  # Ignore 'notes' column

        # Compare DataFrames
        results = compare_dataframes(df_m7, df_ss, primary_key, ignore_pks, ignore_attrs)

        # Print report
        print_report(results)
