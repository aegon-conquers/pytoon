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
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Minimal Test Section ---
def minimal_test(url: str):
    """
    Minimal test to verify API access with verify=False and application/csv handling.
    """
    print("WARNING: SSL verification disabled - insecure, use only for testing")
    try:
        response = requests.get(url, timeout=10, verify=False)
        response.raise_for_status()
        
        content_type = response.headers.get("Content-Type", "").lower()
        print(f"Content-Type: {content_type}")
        
        if "application/csv" in content_type or "text/csv" in content_type:
            csv_data = StringIO(response.content.decode("utf-8"))
            df = pd.read_csv(csv_data)
            print(f"Success: Loaded {len(df)} rows")
        else:
            print(f"Unexpected Content-Type: {content_type}")
            
    except requests.exceptions.SSLError as e:
        print(f"SSL Error: {e}")
    except requests.exceptions.RequestException as e:
        print(f"Request Error: {e}")
    except pd.errors.ParserError as e:
        print(f"CSV Parsing Error: {e}")

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
    Compare two DataFrames row by row and column by column using separate ID columns.
    Assumes SingleStore ID is unique; compares all matching M7 records, ignoring unmatched.
    
    Args:
        df1: First DataFrame (M7)
        df2: Second DataFrame (SingleStore)
        m7_id_column: ID column name for M7 DataFrame (duplicates allowed)
        singlestore_id_column: ID column name for SingleStore DataFrame (assumed unique)
        
    Returns:
        Dictionary containing comparison results
    """
    comparison_results = {
        "column_differences": [],
        "row_count_difference": None,
        "value_differences": [],
        "missing_rows_m7": [],
        "missing_rows_singlestore": [],
        "error": None
    }

    if df1 is None or df2 is None:
        comparison_results["error"] = "Comparison skipped due to missing data"
        return comparison_results
    
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
        logger.warning(f"Column differences detected: {comparison_results['column_differences']}")
    else:
        common_cols = list(cols1)

    # Check if ID columns exist
    if m7_id_column not in df1.columns:
        comparison_results["error"] = f"M7 ID column '{m7_id_column}' not found in M7 DataFrame"
        logger.warning(f"M7 ID column '{m7_id_column}' not in M7 columns: {list(df1.columns)}")
        return comparison_results
    if singlestore_id_column not in df2.columns:
        comparison_results["error"] = f"SingleStore ID column '{singlestore_id_column}' not found in SingleStore DataFrame"
        logger.warning(f"SingleStore ID column '{singlestore_id_column}' not in SingleStore columns: {list(df2.columns)}")
        return comparison_results

    # Check for duplicate IDs in SingleStore only (assumed unique)
    singlestore_duplicates = df2[singlestore_id_column].duplicated(keep=False)
    if singlestore_duplicates.any():
        duplicate_ids = df2[singlestore_id_column][singlestore_duplicates].unique().tolist()
        comparison_results["error"] = f"SingleStore ID column '{singlestore_id_column}' contains duplicates: {duplicate_ids}"
        logger.warning(f"Duplicate IDs in SingleStore DataFrame for '{singlestore_id_column}': {duplicate_ids}")
        return comparison_results

    comparison_results["row_count_difference"] = {
        "m7_rows": len(df1),
        "singlestore_rows": len(df2)
    }

    try:
        # Rename SingleStore ID column to match M7's for merging
        df2_mapped = df2.rename(columns={singlestore_id_column: m7_id_column})
        
        # Inner merge to keep only rows with matching IDs, preserving M7 duplicates
        merged = df1.merge(df2_mapped, on=m7_id_column, how='inner', suffixes=('_m7', '_singlestore'))
        
        # Log if M7 has duplicates that were matched
        m7_duplicates = merged[m7_id_column].duplicated(keep=False)
        if m7_duplicates.any():
            duplicate_count = m7_duplicates.sum() // 2  # Each duplicate pair counts once
            logger.info(f"M7 DataFrame has {duplicate_count} IDs with multiple matches to SingleStore")

        # Compare values for all matched rows
        for col in common_cols:
            if col == m7_id_column:  # Skip the ID column itself
                continue
            m7_col = f"{col}_m7"
            singlestore_col = f"{col}_singlestore"
            if m7_col in merged.columns and singlestore_col in merged.columns:
                differences = merged[merged[m7_col].astype(str) != merged[singlestore_col].astype(str)]
                for _, row in differences.iterrows():
                    comparison_results["value_differences"].append({
                        "row_id": row[m7_id_column],
                        "column": col,
                        "m7_value": row[m7_col],
                        "singlestore_value": row[singlestore_col]
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

                html_content.append("<div class='section'>")
                html_content.append("<h2>Row Count Comparison</h2>")
                row_diff = results["row_count_difference"]
                html_content.append("<table>")
                html_content.append("<tr><th>M7 Rows</th><th>SingleStore Rows</th></tr>")
                html_content.append(f"<tr><td>{row_diff['m7_rows']}</td><td>{row_diff['singlestore_rows']}</td></tr>")
                html_content.append("</table>")
                html_content.append("</div>")

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
    
    print("Comparison complete. Reports generated:")
    for path in report_paths:
        print(f"- {path}")

if __name__ == "__main__":
    main()
