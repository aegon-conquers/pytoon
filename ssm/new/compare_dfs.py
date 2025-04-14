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
            html_content.append(f"<p class='error'>Comparison failed: {results['error']}</p>")
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
