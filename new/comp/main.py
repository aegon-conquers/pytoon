import pandas as pd
from io import StringIO
from dataframe_comparator import fetch_csv_from_api, compare_dataframes, print_report, write_html_report

# Example usage
if __name__ == "__main__":
    # Sample API URLs and keys
    m7_url = "https://api.example.com/{endpoint_key}/m7.csv"
    m7_url_keys = ['A1', 'A2']
    ss_url = "https://api.example.com/{endpoint_key}/ss.csv"
    ss_url_keys = ['A1', 'A2']

    # Sample DataFrames for testing (with mismatched row counts)
    csv_m7_a1 = """id,dept_id,name,age,city,notes
1,10,Alice,25,New York,Note1
2,20,Bob,25,Los Angeles,Note2
3,30,Charlie,45,Chicago,Note3
"""
    csv_m7_a2 = """id,dept_id,name,age,city,notes
4,40,David,45,Houston,Note4
5,50,Eve,45,Seattle,Note5
6,60,Frank,50,Denver,Note6
"""
    csv_ss_a1 = """id,dept_id,name,age,city,notes
1,10,Alice,26,New York,Note1
2,20,Bob,26,Los Angeles,Note2
3,30,Charlie,666,Chicagos,Note3
"""
    csv_ss_a2 = """id,dept_id,name,age,city,notes
4,40,David,666,Houstons,Note4
7,70,Gina,55,Miami,Note7
"""

    # Simulate API responses for testing
    primary_key = ['id', 'dept_id']
    df_m7_a1 = pd.read_csv(StringIO(csv_m7_a1), dtype={pk: str for pk in primary_key})
    df_m7_a2 = pd.read_csv(StringIO(csv_m7_a2), dtype={pk: str for pk in primary_key})
    df_ss_a1 = pd.read_csv(StringIO(csv_ss_a1), dtype={pk: str for pk in primary_key})
    df_ss_a2 = pd.read_csv(StringIO(csv_ss_a2), dtype={pk: str for pk in primary_key})
    df_m7 = pd.concat([df_m7_a1, df_m7_a2], ignore_index=True)
    df_ss = pd.concat([df_ss_a1, df_ss_a2], ignore_index=True)
    # Simulate row counts
    m7_row_counts = {'A1': len(df_m7_a1), 'A2': len(df_m7_a2)}
    ss_row_counts = {'A1': len(df_ss_a1), 'A2': len(df_ss_a2)}

    # Uncomment to fetch from real APIs
    # df_m7, m7_row_counts = fetch_csv_from_api(m7_url, m7_url_keys, primary_key)
    # df_ss, ss_row_counts = fetch_csv_from_api(ss_url, ss_url_keys, primary_key)

    # Check if DataFrames were loaded successfully
    if df_m7 is None or df_ss is None:
        print("Failed to load one or both DataFrames.")
    else:
        # Define primary key and ignore lists
        ignore_pks = []  # Empty to match your scenario
        ignore_attrs = ['notes']

        # Compare DataFrames
        results = compare_dataframes(df_m7, df_ss, primary_key, ignore_pks, ignore_attrs)

        # Print console report
        print_report(results, m7_row_counts, ss_row_counts)

        # Write HTML report
        write_html_report(results, m7_row_counts, ss_row_counts, "comparison_report.html")
