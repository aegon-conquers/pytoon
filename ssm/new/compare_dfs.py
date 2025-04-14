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

        # Get non-ID columns for comparison (assume same columns except ID names)
        m7_cols = [col for col in df1.columns if col != m7_id_column]
        ss_cols = [col for col in df2.columns if col != singlestore_id_column]
        common_cols = sorted(set(m7_cols) & set(ss_cols))  # Columns to compare
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
            m7_records = m7_dict.get(ss_id, [])  # Match SingleStore ID to M7 ID
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
