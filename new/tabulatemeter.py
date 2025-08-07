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
    """Compare two DataFrames and generate comparison statistics."""
    # Initialize results dictionary
    results = {
        'row_count_comparison': {},
        'mismatch_counts': {},
        'mismatch_samples': {}  # Dictionary to store samples per attribute
    }

    # Filter out ignored primary keys
    df_m7 = df_m7[~df_m7[primary_key].isin(ignore_pks)].copy()
    df_ss = df_ss[~df_ss[primary_key].isin(ignore_pks)].copy()

    # 1. Row count comparison
    results['row_count_comparison'] = {
        'm7_rows': len(df_m7),
        'ss_rows': len(df_ss),
        'match': len(df_m7) == len(df_ss)
    }

    # Verify headers are identical
    if list(df_m7.columns) != list(df_ss.columns):
        print("Error: DataFrame headers do not match.")
        print(f"M7 headers: {list(df_m7.columns)}")
        print(f"SS headers: {list(df_ss.columns)}")
        return None

    # Verify primary key exists
    if primary_key not in df_m7.columns or primary_key not in df_ss.columns:
        print(f"Error: Primary key '{primary_key}' not found in one or both DataFrames.")
        return None

    # Ensure primary keys align
    if not df_m7[primary_key].equals(df_ss[primary_key]):
        print("Error: Primary key values do not match between DataFrames after filtering.")
        return None

    # 2. Count mismatched values per attribute and collect 5 samples per attribute
    mismatch_counts = {}
    mismatch_samples = {col: [] for col in df_m7.columns if col != primary_key and col not in ignore_attrs}
    max_samples_per_attr = 5

    for col in df_m7.columns:
        if col != primary_key and col not in ignore_attrs:
            mismatches = 0
            for row in range(len(df_m7)):
                val_m7 = df_m7[col].iloc[row]
                val_ss = df_ss[col].iloc[row]
                if str(val_m7) != str(val_ss) or (pd.isna(val_m7) != pd.isna(val_ss)):
                    mismatches += 1
                    if len(mismatch_samples[col]) < max_samples_per_attr:
                        pk_value = df_m7[primary_key].iloc[row]
                        mismatch_samples[col].append({
                            'primary_key': pk_value,
                            'm7_value': val_m7,
                            'ss_value': val_ss
                        })
            mismatch_counts[col] = mismatches
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
