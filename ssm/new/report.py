import pandas as pd
import requests
import logging
import os
from typing import Dict, Optional
from urllib.parse import urlparse

# Setup logging
logging.basicConfig(
    filename="logs/comparison.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class DataComparator:
    def __init__(self, m7_urls_file: str, singlestore_urls_file: str, reports_dir: str = "reports"):
        self.m7_urls_file = m7_urls_file
        self.singlestore_urls_file = singlestore_urls_file
        self.reports_dir = reports_dir
        os.makedirs(self.reports_dir, exist_ok=True)
        os.makedirs("logs", exist_ok=True)

    def fetch_api_data(self, url: str) -> Optional[pd.DataFrame]:
        """
        Fetch CSV data from API URL and return as DataFrame.
        """
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            df = pd.read_csv(pd.compat.StringIO(response.text), encoding="utf-8")
            logger.info(f"Fetched data from {url}, rows: {len(df)}")
            return df
        except Exception as e:
            logger.error(f"Failed to fetch data from {url}: {str(e)}")
            return None

    def compare_dataframes(self, df1: pd.DataFrame, df2: pd.DataFrame, m7_id_column: str, singlestore_id_column: str) -> Dict:
        """
        Compare M7 and SingleStore responses using dictionaries, joining SingleStore's ID to M7's ID.
        Compares all M7 records matching SingleStore IDs, allowing M7 duplicates.
        
        Args:
            df1: M7 DataFrame
            df2: SingleStore DataFrame
            m7_id_column: M7 ID column (may have duplicates)
            singlestore_id_column: SingleStore ID column (unique)
            
        Returns:
            Dictionary containing comparison results
        """
        comparison_results = {
            "column_differences": [],
            "row_count_difference": None,
            "value_differences": [],
            "error": None
        }

        if df1 is None or df2 is None:
            comparison_results["error"] = "Comparison skipped due to missing data"
            logger.warning("One or both DataFrames are None")
            return comparison_results

        try:
            # Log columns
            logger.info(f"M7 columns: {list(df1.columns)}")
            logger.info(f"SingleStore columns: {list(df2.columns)}")

            # Validate ID columns
            if m7_id_column not in df1.columns:
                comparison_results["error"] = f"M7 ID column '{m7_id_column}' not found"
                logger.warning(f"M7 ID column '{m7_id_column}' not in {list(df1.columns)}")
                return comparison_results
            if singlestore_id_column not in df2.columns:
                comparison_results["error"] = f"SingleStore ID column '{singlestore_id_column}' not found"
                logger.warning(f"SingleStore ID column '{singlestore_id_column}' not in {list(df2.columns)}")
                return comparison_results

            # Get non-ID columns for comparison
            m7_cols = [col for col in df1.columns if col != m7_id_column]
            ss_cols = [col for col in df2.columns if col != singlestore_id_column]
            common_cols = sorted(set(m7_cols) & set(ss_cols))
            logger.info(f"Columns to compare: {common_cols}")

            if not common_cols:
                comparison_results["error"] = "No common non-ID columns to compare"
                logger.warning("No overlapping non-ID columns")
                return comparison_results

            # Build SingleStore dictionary: {id: {col: value, ...}}
            singlestore_dict = {}
            for _, row in df2.iterrows():
                if pd.isna(row[singlestore_id_column]):
                    logger.warning(f"Null ID in SingleStore '{singlestore_id_column}'")
                    continue
                ss_id = row[singlestore_id_column]
                if ss_id in singlestore_dict:
                    comparison_results["error"] = f"SingleStore ID '{ss_id}' is not unique"
                    logger.warning(f"Duplicate SingleStore ID: {ss_id}")
                    return comparison_results
                singlestore_dict[ss_id] = {col: row[col] for col in ss_cols}

            # Build M7 dictionary: {id: [{col: value, ...}, ...]}
            m7_dict = {}
            for _, row in df1.iterrows():
                if pd.isna(row[m7_id_column]):
                    logger.warning(f"Null ID in M7 '{m7_id_column}'")
                    continue
                m7_id = row[m7_id_column]
                if m7_id not in m7_dict:
                    m7_dict[m7_id] = []
                m7_dict[m7_id].append({col: row[col] for col in m7_cols})

            logger.info(f"SingleStore records: {len(singlestore_dict)}")
            logger.info(f"M7 IDs: {list(m7_dict.keys())}")

            comparison_results["row_count_difference"] = {
                "m7_rows": sum(len(recs) for recs in m7_dict.values()),
                "singlestore_rows": len(singlestore_dict)
            }

            # Compare: Iterate SingleStore records, find all M7 matches
            for ss_id, ss_record in singlestore_dict.items():
                m7_records = m7_dict.get(ss_id, [])
                if not m7_records:
                    logger.info(f"No M7 records for SingleStore ID {ss_id}")
                    continue
                for m7_record in m7_records:
                    for col in common_cols:
                        ss_value = ss_record[col]
                        m7_value = m7_record[col]
                        if str(ss_value) != str(m7_value):
                            comparison_results["value_differences"].append({
                                "row_id": ss_id,
                                "column": col,
                                "m7_value": m7_value,
                                "singlestore_value": ss_value
                            })

        except Exception as e:
            comparison_results["error"] = f"Comparison failed: {str(e)}"
            logger.error(f"Comparison error: {str(e)}")
            return comparison_results

        return comparison_results

    def generate_html_report(self, results: Dict, report_path: str) -> None:
        """
        Generate an HTML report from comparison results.
        
        Args:
            results: Dictionary containing comparison results
            report_path: Path to save the HTML report
        """
        try:
            html_content = ["<html><head><title>Comparison Report</title></head><body>"]
            html_content.append("<h1>Comparison Report</h1>")

            if results.get("error"):
                html_content.append(f"<p style='color:red'>Error: {results['error']}</p>")
                html_content.append("</body></html>")
                with open(report_path, "w", encoding="utf-8") as f:
                    f.write("\n".join(html_content))
                return

            # Row count difference
            if results.get("row_count_difference"):
                m7_rows = results["row_count_difference"].get("m7_rows", 0)
                ss_rows = results["row_count_difference"].get("singlestore_rows", 0)
                html_content.append("<h2>Row Counts</h2>")
                html_content.append(f"<p>M7 Rows: {m7_rows}</p>")
                html_content.append(f"<p>SingleStore Rows: {ss_rows}</p>")

            # Value differences
            if results.get("value_differences"):
                html_content.append("<h2>Value Differences</h2>")
                html_content.append("<table border='1'>")
                html_content.append("<tr><th>Row ID</th><th>Column</th><th>M7 Value</th><th>SingleStore Value</th></tr>")
                for diff in results["value_differences"]:
                    html_content.append(
                        f"<tr><td>{diff['row_id']}</td><td>{diff['column']}</td>"
                        f"<td>{diff['m7_value']}</td><td>{diff['singlestore_value']}</td></tr>"
                    )
                html_content.append("</table>")
            else:
                html_content.append("<p>No differences found.</p>")

            html_content.append("</body></html>")
            with open(report_path, "w", encoding="utf-8") as f:
                f.write("\n".join(html_content))
            logger.info(f"HTML report generated at {report_path}")

        except Exception as e:
            logger.error(f"Failed to generate HTML report: {str(e)}")
            raise

    def run_comparison(self) -> None:
        """
        Run comparison for all URLs in the input files.
        """
        try:
            # Read URLs and ID columns
            with open(self.m7_urls_file, "r") as f:
                m7_urls = [line.strip().split(",") for line in f if line.strip()]
            with open(self.singlestore_urls_file, "r") as f:
                singlestore_urls = [line.strip().split(",") for line in f if line.strip()]

            for m7_entry, ss_entry in zip(m7_urls, singlestore_urls):
                if len(m7_entry) != 2 or len(ss_entry) != 2:
                    logger.error(f"Invalid URL format: M7: {m7_entry}, SingleStore: {ss_entry}")
                    continue

                m7_url, m7_id_col = m7_entry
                ss_url, ss_id_col = ss_entry

                # Fetch data
                m7_df = self.fetch_api_data(m7_url)
                ss_df = self.fetch_api_data(ss_url)

                if m7_df is None or ss_df is None:
                    logger.error(f"Skipping comparison for {m7_url} and {ss_url} due to fetch failure")
                    continue

                # Compare
                results = self.compare_dataframes(m7_df, ss_df, m7_id_col, ss_id_col)

                # Generate report
                report_name = f"report_{urlparse(m7_url).path.replace('/', '_')}.html"
                report_path = os.path.join(self.reports_dir, report_name)
                self.generate_html_report(results, report_path)

        except Exception as e:
            logger.error(f"Comparison run failed: {str(e)}")

if __name__ == "__main__":
    comparator = DataComparator("m7_urls.txt", "singlestore_urls.txt")
    comparator.run_comparison()
