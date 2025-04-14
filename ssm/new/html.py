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
        "table { border-collapse: collapse; width: 100%; margin-top: 10px; table-layout: fixed; }",
        "th, td { border: 1px solid #ddd; padding: 8px; text-align: left; overflow-wrap: break-word; }",
        "th { background-color: #f2f2f2; }",
        "tr:nth-child(even) { background-color: #f9f9f9; }",
        ".section { margin-bottom: 20px; }",
        ".error { color: red; font-weight: bold; }",
        "th:nth-child(1), td:nth-child(1) { width: 15%; }",  # Row ID
        "th:nth-child(2), td:nth-child(2) { width: 20%; }",  # Column
        "th:nth-child(3), td:nth-child(3) { width: 25%; max-width: 200px; white-space: normal; }",  # M7 Value
        "th:nth-child(4), td:nth-child(4) { width: 25%; max-width: 200px; white-space: normal; }",  # SingleStore Value
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
