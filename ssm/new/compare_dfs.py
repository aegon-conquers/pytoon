def compare_dataframes(self, df1: pd.DataFrame, df2: pd.DataFrame, m7_id_column: str, singlestore_id_column: str) -> Dict:
    """
    Compare M7 and SingleStore DataFrames, joining SingleStore's ID to M7's ID.
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
    
    logger.info(f"M7 columns: {list(df1.columns)}")
    logger.info(f"SingleStore columns: {list(df2.columns)}")

    # Check if ID columns exist
    if m7_id_column not in df1.columns:
        comparison_results["error"] = f"M7 ID column '{m7_id_column}' not found"
        logger.warning(f"M7 ID column '{m7_id_column}' not in {list(df1.columns)}")
        return comparison_results
    if singlestore_id_column not in df2.columns:
        comparison_results["error"] = f"SingleStore ID column '{singlestore_id_column}' not found"
        logger.warning(f"SingleStore ID column '{singlestore_id_column}' not in {list(df2.columns)}")
        return comparison_results

    # Check SingleStore ID uniqueness
    if df2[singlestore_id_column].duplicated().any():
        duplicate_ids = df2[singlestore_id_column][df2[singlestore_id_column].duplicated()].unique().tolist()
        comparison_results["error"] = f"SingleStore ID column '{singlestore_id_column}' has duplicates: {duplicate_ids}"
        logger.warning(f"SingleStore duplicates: {duplicate_ids}")
        return comparison_results

    comparison_results["row_count_difference"] = {
        "m7_rows": len(df1),
        "singlestore_rows": len(df2)
    }

    try:
        # Inner merge: SingleStore's ID (c1) to M7's ID (c2)
        merged = df2.merge(df1, left_on=singlestore_id_column, right_on=m7_id_column, how='inner', suffixes=('_singlestore', '_m7'))
        
        logger.info(f"Merged columns: {list(merged.columns)}")
        
        # Log M7 duplicates
        if merged[m7_id_column].duplicated().any():
            duplicate_count = merged[m7_id_column].duplicated().sum()
            logger.info(f"M7 has {duplicate_count} duplicate IDs matched to SingleStore")

        # Compare non-ID columns (c3, c4)
        common_cols = ['c3', 'c4']  # Since c1, c2 are IDs
        for col in common_cols:
            singlestore_col = f"{col}_singlestore"
            m7_col = f"{col}_m7"
            if singlestore_col in merged.columns and m7_col in merged.columns:
                differences = merged[merged[singlestore_col].astype(str) != merged[m7_col].astype(str)]
                for _, row in differences.iterrows():
                    comparison_results["value_differences"].append({
                        "row_id": row[singlestore_id_column],
                        "column": col,
                        "m7_value": row[m7_col],
                        "singlestore_value": row[singlestore_col]
                    })

    except Exception as e:
        comparison_results["error"] = f"Comparison failed: {str(e)}"
        logger.error(f"Comparison error: {str(e)}")
        
    return comparison_results
