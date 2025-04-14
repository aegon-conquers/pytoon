import requests
import pandas as pd
from io import StringIO
import logging
import datetime
import os
from typing import Tuple, Dict, List
import html
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/comparison.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Minimal Test Section ---
def minimal_test(url: str):
    """
    Minimal test to verify API access with verify=False and application/csv handling.
    """
    logger.warning("SSL verification disabled - insecure, use only for testing")
    try:
        response = requests.get(url, timeout=10, verify=False)
        response.raise_for_status()
        
        content_type = response.headers.get("Content-Type", "").lower()
        logger.info(f"Content-Type: {content_type}")
        
        if "application/csv" in content_type or "text/csv" in content_type:
            csv_data = StringIO(response.content.decode("utf-8"))
            df = pd.read_csv(csv_data)
            logger.info(f"Success: Loaded {len(df)} rows")
        else:
            logger.error(f"Unexpected Content-Type: {content_type}")
            
    except requests.exceptions.SSLError as e:
        logger.error(f"SSL Error: {e}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Request Error: {e}")
    except pd.errors.ParserError as e:
        logger.error(f"CSV Parsing Error: {e}")

# --- Main Script Section ---
class APIDataComparator:
    def __init__(self, m7_urls_file: str, singlestore_urls_file: str, output_dir: str = "reports"):
        """
        Initialize the comparator with files containing API endpoints and ID columns.
        
        Args:
            m7_urls_file: Path to file with M7 API URLs and ID columns (format: 'url,id_column')
            singlestore_urls_file: Path to file with SingleStore API URLs and ID columns
            output_dir: Directory to save comparison reports
        """
        self.m7_urls_file = m7_urls_file
        self.singlestore_urls_file = singlestore_urls_file
        self.output_dir = output_dir
        
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs("logs", exist_ok=True)
        
        self.m7_urls = self.read_urls(m7_urls_file)
        self.singlestore_urls = self.read_urls(singlestore_urls_file)
        
        if len(self.m7_urls) != len(self.singlestore_urls):
            raise ValueError("M7 and SingleStore URL files must have the same number of entries")
        if not self.m7_urls:
            raise ValueError("URL files are empty")

    def read_urls(self, file_path: str) -> List[Tuple[str, str]]:
        """
        Read URLs and ID columns from a file, one per line, in format 'url,id_column'.
        
        Returns:
            List of (URL, id_column) tuples
        """
        try:
            with open(file_path, 'r') as f:
                lines = [line.strip() for line in f if line.strip()]
            urls = []
            for line in lines:
                if ',' not in line:
                    logger.error(f"Invalid format in {file_path}: {line}. Expected 'url,id_column'")
                    raise ValueError(f"Invalid format: {line}")
                url, id_column = line.split(',', 1)
                parsed = urlparse(url)
                if not (parsed.scheme in ('http', 'https') and parsed.netloc):
                    logger.error(f"Invalid URL in {file_path}: {url}")
                    raise ValueError(f"Invalid URL: {url}")
                if not id_column:
                    logger.error(f"Missing id_column in {file_path}: {line}")
                    raise ValueError(f"Missing id_column: {line}")
                urls.append((url, id_column))
            logger.info(f"Read {len(urls)} URLs with ID columns from {file_path}")
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
        status = {"success": False, "error": None, "rows": 0, "url": url}
        try:
            logger.warning(f"SSL verification disabled for {url} - insecure, use only for testing")
            response = requests.get(url, timeout=10, verify=False)
            response.raise_for_status()
            
            if not response.content:
                status["error"] = "Empty response received"
                logger.warning(f"Empty response from {url}")
                return None, status
            
            content_type = response.headers.get("Content-Type", "").lower()
            if "application/csv" in content_type or "text/csv" in content_type:
                csv_data = StringIO(response.content.decode("utf-8"))
            else:
                status["error"] = f"Unexpected Content-Type: {content_type}"
                logger.warning(f"Unexpected Content-Type from {url}: {content_type}")
                return None, status
                
            df = pd.read_csv(csv_data)
            
            if df.empty:
                status["error"] = "Empty CSV data (no rows)"
                logger.warning(f"Empty CSV from {url}")
                return None, status
                
            status["success"] = True
            status["rows"] = len(df)
            logger.info(f"Successfully fetched {len(df)} rows from {url}")
            return df, status
            
        except requests.exceptions.SSLError as e:
            status["error"] = f"SSL error: {str(e)}. Possible causes: untrusted certificate or network issue."
            logger.error(f"SSL error fetching {url}: {str(e)}")
            return None, status
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

            # Check for column differences
            if set(m7_cols) != set(ss_cols):
                comparison_results["column_differences"] = {
                    "m7_only": list(set(m7_cols) - set(ss_cols)),
                    "singlestore_only": list(set(ss_cols) - set(m7_cols))
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

    def generate_html_report(self, results: Dict, m7_url: str, singlestore_url: str, m7_status: Dict, singlestore_status: Dict, index: int) -> str:
        """
        Generate an HTML comparison report, including error details.
        """
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = os.path.join(self.output_dir, f"comparison_report_{index}_{timestamp}.html")
        
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

        if m7_status["success"] and singlestore_status["success"]:
            if results.get("error"):
                html_content.append("<div class='section'>")
                html_content.append("<h2>Comparison Result</h2>")
                html_content.append(f"<p class='error'>Comparison failed: {html.escape(str(results['error']))}</p>")
                html_content.append("</div>")
            else:
                html_content.append("<div class='section'>")
                html_content.append("<h2>Column Differences</h2>")
                if results.get("column_differences"):
                    html_content.append("<table>")
                    html_content.append("<tr><th>M7 Only</th><th>SingleStore Only</th></tr>")
                    html_content.append("<tr>")
                    html_content.append(f"<td>{html.escape(str(results['column_differences'].get('m7_only', [])))}</td>")
                    html_content.append(f"<td>{html.escape(str(results['column_differences'].get('singlestore_only', [])))}</td>")
                    html_content.append("</tr>")
                    html_content.append("</table>")
                else:
                    html_content.append("<p>No column differences detected.</p>")
                html_content.append("</div>")

                html_content.append("<div class='section'>")
                html_content.append("<h2>Row Count Comparison</h2>")
                row_diff = results.get("row_count_difference", {})
                html_content.append("<table>")
                html_content.append("<tr><th>M7 Rows</th><th>SingleStore Rows</th></tr>")
                html_content.append(f"<tr><td>{row_diff.get('m7_rows', 0)}</td><td>{row_diff.get('singlestore_rows', 0)}</td></tr>")
                html_content.append("</table>")
                html_content.append("</div>")

                html_content.append("<div class='section'>")
                html_content.append("<h2>Value Differences</h2>")
                if results.get("value_differences"):
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

        html_content.append("</body>")
        html_content.append("</html>")

        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("\n".join(html_content))

        logger.info(f"HTML report generated at {report_path}")
        return report_path

    def run_comparison(self) -> List[str]:
        """
        Run the comparison for all pairs of API endpoints.
        
        Returns:
            List of paths to generated HTML reports
        """
        report_paths = []
        
        for i, ((m7_url, m7_id_column), (singlestore_url, singlestore_id_column)) in enumerate(zip(self.m7_urls, self.singlestore_urls), 1):
            logger.info(f"Comparing API pair {i}: M7={m7_url} (ID: {m7_id_column}), SingleStore={singlestore_url} (ID: {singlestore_id_column})")
            m7_df, m7_status = self.fetch_api_data(m7_url)
            singlestore_df, singlestore_status = self.fetch_api_data(singlestore_url)
            
            comparison_results = self.compare_dataframes(m7_df, singlestore_df, m7_id_column, singlestore_id_column)
            
            report_path = self.generate_html_report(comparison_results, m7_url, singlestore_url, m7_status, singlestore_status, i)
            report_paths.append(report_path)
        
        return report_paths

def main():
    # Minimal test example - uncomment to run standalone test
    # minimal_test("https://your-api-url.com")  # Replace with one of your API URLs
    
    # Main script execution
    m7_urls_file = "m7_urls.txt"
    singlestore_urls_file = "singlestore_urls.txt"
    
    comparator = APIDataComparator(
        m7_urls_file=m7_urls_file,
        singlestore_urls_file=singlestore_urls_file
    )
    
    report_paths = comparator.run_comparison()
    
    logger.info("Comparison complete. Reports generated:")
    for path in report_paths:
        logger.info(f"- {path}")

if __name__ == "__main__":
    main()
