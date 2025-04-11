import requests
import pandas as pd
from io import StringIO
import logging
import datetime
import os
from typing import Tuple, Dict, List
import html
from urllib.parse import urlparse
import validators  # New dependency for URL validation

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class APIDataComparator:
    def __init__(self, m7_urls_file: str, singlestore_urls_file: str, output_dir: str = "reports"):
        """
        Initialize the comparator with files containing API endpoints.
        """
        self.m7_urls_file = m7_urls_file
        self.singlestore_urls_file = singlestore_urls_file
        self.output_dir = output_dir
        
        os.makedirs(self.output_dir, exist_ok=True)
        
        self.m7_urls = self.read_urls(m7_urls_file)
        self.singlestore_urls = self.read_urls(singlestore_urls_file)
        
        if len(self.m7_urls) != len(self.singlestore_urls):
            raise ValueError("M7 and SingleStore URL files must have the same number of URLs")
        if not self.m7_urls:
            raise ValueError("URL files are empty")

    def read_urls(self, file_path: str) -> List[str]:
        """
        Read URLs from a file, validate format.
        """
        try:
            with open(file_path, 'r') as f:
                urls = [line.strip() for line in f if line.strip()]
            # Validate URLs
            for url in urls:
                if not validators.url(url):
                    logger.error(f"Invalid URL in {file_path}: {url}")
                    raise ValueError(f"Invalid URL: {url}")
            logger.info(f"Read {len(urls)} valid URLs from {file_path}")
            return urls
        except FileNotFoundError:
            logger.error(f"URL file not found: {file_path}")
            raise
        except Exception as e:
            logger.error(f"Error reading URL file {file_path}: {str(e)}")
            raise

    def fetch_api_data(self, url: str) -> Tuple[pd.DataFrame, Dict]:
        """
        Fetch CSV data from an API endpoint, return DataFrame and status.
        No authentication required.
        
        Args:
            url: API endpoint URL
            
        Returns:
            Tuple of (DataFrame or None, status dictionary with success/error details)
        """
        status = {"success": False, "error": None, "rows": 0}
        try:
            logger.info(f"Fetching data from {url}")
            # Set timeout to avoid hanging
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            # Check for empty response
            if not response.text.strip():
                status["error"] = "Empty response received"
                logger.warning(f"Empty response from {url}")
                return None, status
            
            # Convert CSV string to DataFrame
            csv_data = StringIO(response.text)
            df = pd.read_csv(csv_data)
            
            # Check for empty DataFrame
            if df.empty:
                status["error"] = "Empty CSV data (no rows)"
                logger.warning(f"Empty CSV from {url}")
                return None, status
                
            status["success"] = True
            status["rows"] = len(df)
            logger.info(f"Successfully fetched {len(df)} rows from {url}")
            return df, status
            
        except requests.Timeout:
            status["error"] = "Request timed out after 10 seconds"
            logger.error(f"Timeout fetching {url}")
            return None, status
        except requests.HTTPError as e:
            status["error"] = f"HTTP error: {response.status_code} {response.reason}"
            logger.error(f"HTTP error fetching {url}: {str(e)}")
            return None, status
        except requests.ConnectionError as e:
            status["error"] = f"Network connection error: {str(e)}"
            logger.error(f"Network error fetching {url}")
            return None, status
        except pd.errors.EmptyDataError:
            status["error"] = "No data returned (empty CSV)"
            logger.error(f"No data returned from {url}")
            return None, status
        except pd.errors.ParserError as e:
            status["error"] = f"CSV parsing error: {str(e)}"
            logger.error(f"CSV parsing error for {url}")
            return None, status
        except Exception as e:
            status["error"] = f"Unexpected error: {str(e)}"
            logger.error(f"Unexpected error fetching {url}: {str(e)}")
            return None, status

    def compare_dataframes(self, df1: pd.DataFrame, df2: pd.DataFrame) -> Dict:
        """
        Compare two DataFrames row by row and column by column.
        Handle cases where one or both DataFrames are None.
        """
        comparison_results = {
            "column_differences": [],
            "row_count_difference": None,
            "value_differences": [],
            "missing_rows_m7": [],
            "missing_rows_singlestore": [],
            "error": None
        }

        # Handle cases where one or both DataFrames are missing
        if df1 is None or df2 is None:
            comparison_results["error"] = "Comparison skipped due to missing data"
            return comparison_results
        
        # Compare columns
        cols1, cols2 = set(df1.columns), set(df2.columns)
        if cols1 != cols2:
            comparison_results["column_differences"] = {
                "m7_only": list(cols1 - cols2),
                "singlestore_only": list(cols2 - cols1)
            }
            common_cols = list(cols1 & cols2)
            if not common_cols:
                comparison_results["error"] = "No common columns for comparison"
                logger.warning("No common columns between DataFrames")
                return comparison_results
            df1 = df1[common_cols]
            df2 = df2[common_cols]
            logger.warning(f"Column differences detected: {comparison_results['column_differences']}")
        else:
            common_cols = list(cols1)

        # Compare row counts
        comparison_results["row_count_difference"] = {
            "m7_rows": len(df1),
            "singlestore_rows": len(df2)
        }

        # Check for unique identifier
        id_column = 'id' if 'id' in common_cols else None
        if not id_column:
            comparison_results["error"] = "No unique identifier column ('id') found"
            logger.warning("No 'id' column for row alignment")
            return comparison_results
        
        try:
            # Align rows based on the identifier
            df1.set_index(id_column, inplace=True)
            df2.set_index(id_column, inplace=True)
            
            # Find common and unique indices
            common_ids = df1.index.intersection(df2.index)
            m7_only = df1.index.difference(df2.index)
            singlestore_only = df2.index.difference(df1.index)
            
            if m7_only.any():
                comparison_results["missing_rows_singlestore"] = m7_only.tolist()
                logger.warning(f"Rows in M7 but not in SingleStore: {m7_only.tolist()}")
            if singlestore_only.any():
                comparison_results["missing_rows_m7"] = singlestore_only.tolist()
                logger.warning(f"Rows in SingleStore but not in M7: {singlestore_only.tolist()}")

            # Compare values for common rows
            for idx in common_ids:
                for col in common_cols:
                    val1 = df1.loc[idx, col]
                    val2 = df2.loc[idx, col]
                    
                    # Handle NaN values
                    if pd.isna(val1) and pd.isna(val2):
                        continue
                    # Convert to string for comparison to avoid type mismatches
                    if str(val1) != str(val2):
                        comparison_results["value_differences"].append({
                            "row_id": idx,
                            "column": col,
                            "m7_value": val1,
                            "singlestore_value": val2
                        })
        except Exception as e:
            comparison_results["error"] = f"Comparison failed: {str(e)}"
            logger.error(f"Error during DataFrame comparison: {str(e)}")
        
        return comparison_results

    def generate_html_report(self, results: Dict, m7_url: str, singlestore_url: str, m7_status: Dict, singlestore_status: Dict, index: int) -> str:
        """
        Generate an HTML comparison report, including error details.
        """
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = os.path.join(self.output_dir, f"comparison_report_{index}_{timestamp}.html")
        
        # HTML template
        html_content = [
            "<!DOCTYPE html>",
            "<html>",
            "<head>",
            "<title>API Data Comparison Report</title>",
            "<style>",
            "body { font-family: Arial, sans-serif; margin: 20px; }",
            "h1, h2 { color: #333; }",
            "table { border-collapse: collapse; width: 100%; margin-top: 10px; }",
            "th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }",
            "th { background-color: #f2f2f2; }",
            "tr:nth-child(even) { background-color: #f9f9f9; }",
            ".section { margin-bottom: 20px; }",
            ".error { color: red; font-weight: bold; }",
            "</style>",
            "</head>",
            "<body>",
            "<h1>API Data Comparison Report</h1>",
            f"<p>Generated: {datetime.datetime.now()}</p>",
            f"<p>M7 API: {html.escape(m7_url)}</p>",
            f"<p>SingleStore API: {html.escape(singlestore_url)}</p>",
            "<hr>"
        ]

        # API Status
        html_content.append("<div class='section'>")
        html_content.append("<h2>API Status</h2>")
        html_content.append("<table>")
        html_content.append("<tr><th>API</th><th>Status</th><th>Rows Fetched</th><th>Error</th></tr>")
        html_content.append("<tr>")
        html_content.append(f"<td>M7</td>")
        html_content.append(f"<td>{'Success' if m7_status['success'] else 'Failed'}</td>")
        html_content.append(f"<td>{m7_status['rows']}</td>")
        html_content.append(f"<td>{html.escape(str(m7_status['error']) if m7_status['error'] else 'None')}</td>")
        html_content.append("</tr>")
        html_content.append("<tr>")
        html_content.append(f"<td>SingleStore</td>")
        html_content.append(f"<td>{'Success' if singlestore_status['success'] else 'Failed'}</td>")
        html_content.append(f"<td>{singlestore_status['rows']}</td>")
        html_content.append(f"<td>{html.escape(str(singlestore_status['error']) if singlestore_status['error'] else 'None')}</td>")
        html_content.append("</tr>")
        html_content.append("</table>")
        html_content.append("</div>")

        # Comparison results (only if both APIs succeeded)
        if m7_status["success"] and singlestore_status["success"]:
            if results.get("error"):
                html_content.append("<div class='section'>")
                html_content.append("<h2>Comparison Result</h2>")
                html_content.append(f"<p class='error'>Comparison failed: {html.escape(str(results['error']))}</p>")
                html_content.append("</div>")
            else:
                # Column differences
                html_content.append("<div class='section'>")
                html_content.append("<h2>Column Differences</h2>")
                if results["column_differences"]:
                    html_content.append("<table>")
                    html_content.append("<tr><th>M7 Only</th><th>SingleStore Only</th></tr>")
                    html_content.append("<tr>")
                    html_content.append(f"<td>{html.escape(str(results['column_differences']['m7_only']))}</td>")
                    html_content.append(f"<td>{html.escape(str(results['column_differences']['singlestore_only']))}</td>")
                    html_content.append("</tr>")
                    html_content.append("</table>")
                else:
                    html_content.append("<p>No column differences detected.</p>")
                html_content.append("</div>")

                # Row count differences
                html_content.append("<div class='section'>")
                html_content.append("<h2>Row Count Comparison</h2>")
                row_diff = results["row_count_difference"]
                html_content.append("<table>")
                html_content.append("<tr><th>M7 Rows</th><th>SingleStore Rows</th></tr>")
                html_content.append(f"<tr><td>{row_diff['m7_rows']}</td><td>{row_diff['singlestore_rows']}</td></tr>")
                html_content.append("</table>")
                html_content.append("</div>")

                # Missing rows
                html_content.append("<div class='section'>")
                html_content.append("<h2>Missing Rows</h2>")
                if results["missing_rows_m7"] or results["missing_rows_singlestore"]:
                    html_content.append("<table>")
                    html_content.append("<tr><th>In SingleStore but not M7</th><th>In M7 but not SingleStore</th></tr>")
                    html_content.append("<tr>")
                    html_content.append(f"<td>{html.escape(str(results['missing_rows_m7']))}</td>")
                    html_content.append(f"<td>{html.escape(str(results['missing_rows_singlestore']))}</td>")
                    html_content.append("</tr>")
                    html_content.append("</table>")
                else:
                    html_content.append("<p>No missing rows detected.</p>")
                html_content.append("</div>")

                # Value differences
                html_content.append("<div class='section'>")
                html_content.append("<h2>Value Differences</h2>")
                if results["value_differences"]:
                    html_content.append("<table>")
                    html_content.append("<tr><th>Row ID</th><th>Column</th><th>M7 Value</th><th>SingleStore Value</th></tr>")
                    for diff in results["value_differences"]:
                        html_content.append("<tr>")
                        html_content.append(f"<td>{html.escape(str(diff['row_id']))}</td>")
                        html_content.append(f"<td>{html.escape(str(diff['column']))}</td>")
                        html_content.append(f"<td>{html.escape(str(diff['m7_value']))}</td>")
                        html_content.append(f"<td>{html.escape(str(diff['singlestore_value']))}</td>")
                        html_content.append("</tr>")
                    html_content.append("</table>")
                else:
                    html_content.append("<p>No value differences detected.</p>")
                html_content.append("</div>")
        else:
            html_content.append("<div class='section'>")
            html_content.append("<h2>Comparison Result</h2>")
            html_content.append("<p class='error'>Comparison skipped due to one or both API failures</p>")
            html_content.append("</div>")

        # Close HTML
        html_content.append("</body>")
        html_content.append("</html>")

        # Write to file
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("\n".join(html_content))

        logger.info(f"HTML report generated at {report_path}")
        return report_path

    def run_comparison(self) -> List[str]:
        """
        Run the comparison for all pairs of API endpoints.
        """
        report_paths = []
        
        for i, (m7_url, singlestore_url) in enumerate(zip(self.m7_urls, self.singlestore_urls), 1):
            logger.info(f"Comparing API pair {i}: M7={m7_url}, SingleStore={singlestore_url}")
            m7_df, m7_status = self.fetch_api_data(m7_url)
            singlestore_df, singlestore_status = self.fetch_api_data(singlestore_url)
            
            # Perform comparison if both succeeded, else report errors
            comparison_results = self.compare_dataframes(m7_df, singlestore_df)
            
            # Generate report with status and results
            report_path = self.generate_html_report(comparison_results, m7_url, singlestore_url, m7_status, singlestore_status, i)
            report_paths.append(report_path)
        
        return report_paths

def main():
    m7_urls_file = "m7_urls.txt"
    singlestore_urls_file = "singlestore_urls.txt"
    
    comparator = APIDataComparator(
        m7_urls_file=m7_urls_file,
        singlestore_urls_file=singlestore_urls_file
    )
    
    report_paths = comparator.run_comparison()
    
    print("Comparison complete. Reports generated:")
    for path in report_paths:
        print(f"- {path}")

if __name__ == "__main__":
    main()
