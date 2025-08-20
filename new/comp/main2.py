import yaml
import pandas as pd
from dataframe_comparator import fetch_csv_from_api, compare_dataframes, print_report, write_html_report

if __name__ == "__main__":
    # Load configuration from YAML file
    try:
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        print("Error: config.yaml not found.")
        exit(1)
    except yaml.YAMLError as e:
        print(f"Error: Invalid YAML in config.yaml: {e}")
        exit(1)

    # Extract base URLs
    base_m7_url = config.get('base_m7_url', '')
    base_ss_url = config.get('base_ss_url', '')
    if not base_m7_url or not base_ss_url:
        print("Error: base_m7_url and base_ss_url must be specified in config.yaml.")
        exit(1)

    # Iterate over each endpoint
    for idx, endpoint_config in enumerate(config.get('endpoints', []), 1):
        endpoint = endpoint_config.get('endpoint', '')
        m7_url_keys = endpoint_config.get('m7_url_keys', [])
        ss_url_keys = endpoint_config.get('ss_url_keys', [])
        primary_key = endpoint_config.get('primary_key', [])
        ignore_pks = endpoint_config.get('ignore_pks', [])
        ignore_attrs = endpoint_config.get('ignore_attrs', [])

        # Validate endpoint configuration
        if not endpoint or not m7_url_keys or not ss_url_keys or not primary_key:
            print(f"Error: Invalid configuration for endpoint {idx}: {endpoint}. "
                  f"Required fields: endpoint, m7_url_keys, ss_url_keys, primary_key.")
            continue

        print(f"\n=== Processing Endpoint {idx}: {endpoint} ===")

        # Construct URLs for M7 and SS
        m7_url = f"{base_m7_url}{{endpoint_key}}/{endpoint}"
        ss_url = f"{base_ss_url}{{endpoint_key}}/{endpoint}"

        # Fetch data from APIs
        df_m7, m7_row_counts = fetch_csv_from_api(m7_url, m7_url_keys, primary_key)
        df_ss, ss_row_counts = fetch_csv_from_api(ss_url, ss_url_keys, primary_key)

        # Check if DataFrames were loaded successfully
        if df_m7 is None or df_ss is None:
            print(f"Failed to load one or both DataFrames for endpoint {endpoint}.")
            continue

        # Compare DataFrames
        results = compare_dataframes(df_m7, df_ss, primary_key, ignore_pks, ignore_attrs)
        if results is None:
            print(f"Comparison failed for endpoint {endpoint}.")
            continue

        # Generate reports
        print_report(results, m7_row_counts, ss_row_counts)
        output_file = f"comparison_report_endpoint_{idx}_{endpoint.replace('.', '_')}.html"
        write_html_report(results, m7_row_counts, ss_row_counts, output_file)
