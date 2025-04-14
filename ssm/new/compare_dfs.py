def compare_dataframes(self, df1: pd.DataFrame, df2: pd.DataFrame, m7_id_column: str, singlestore_id_column: str) -> Dict:
    """
    Compare M7 and SingleStore responses using dictionaries, joining SingleStore's ID to M7's ID.
    Compares all M7 records matching SingleStore IDs, allowing M7 duplicates.
    
    Args:
        df1: M7 DataFrame
        df2: SingleStore DataFrame
        m7_id_column: M7 ID column (e.g., 'c2', may have duplicates)
        singlestore_id_column: SingleStore ID column (e.g., 'c1', unique)
        
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
        # Convert DataFrames to dictionaries
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

        # Build SingleStore dictionary: {c1: {c2, c3, c4}}
        singlestore_dict = {}
        for _, row in df2.iterrows():
            if pd.isna(row[singlestore_id_column]):
                logger.warning(f"Null ID in SingleStore '{singlestore_id_column}'")
                continue
            c1 = row[singlestore_id_column]
            if c1 in singlestore_dict:
                comparison_results["error"] = f"SingleStore ID '{c1}' is not unique"
                logger.warning(f"Duplicate SingleStore ID: {c1}")
                return comparison_results
            singlestore_dict[c1] = {
                "c2": row["c2"],
                "c3": row["c3"],
                "c4": row["c4"]
            }

        # Build M7 dictionary: {c2: [{c1, c3, c4}, ...]}
        m7_dict = {}
        for _, row in df1.iterrows():
            if pd.isna(row[m7_id_column]):
                logger.warning(f"Null ID in M7 '{m7_id_column}'")
                continue
            c2 = row[m7_id_column]
            if c2 not in m7_dict:
                m7_dict[c2] = []
            m7_dict[c2].append({
                "c1": row["c1"],
                "c3": row["c3"],
                "c4": row["c4"]
            })

        logger.info(f"SingleStore records: {len(singlestore_dict)}")
        logger.info(f"M7 IDs: {list(m7_dict.keys())}")

        comparison_results["row_count_difference"] = {
            "m7_rows": sum(len(recs) for recs in m7_dict.values()),
            "singlestore_rows": len(singlestore_dict)
        }

        # Compare: Iterate SingleStore records, find all M7 matches
        for c1, ss_record in singlestore_dict.items():
            m7_records = m7_dict.get(c1, [])  # Match c1 to c2
            if not m7_records:
                logger.info(f"No M7 records for SingleStore ID {c1}")
                continue
            for m7_record in m7_records:
                for col in ["c3", "c4"]:
                    ss_value = ss_record[col]
                    m7_value = m7_record[col]
                    if str(ss_value) != str(m7_value):
                        comparison_results["value_differences"].append({
                            "row_id": c1,
                            "column": col,
                            "m7_value": m7_value,
                            "singlestore_value": ss_value
                        })

    except Exception as e:
        comparison_results["error"] = f"Comparison failed: {str(e)}"
        logger.error(f"Comparison error: {str(e)}")
        return comparison_results

    return comparison_results
