import logging
from typing import List, Dict, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

def aggregate_by_columns(
        df: DataFrame,
        group_by_cols: List[str],
        agg_dict: Dict[str, str]
) -> DataFrame:
    
    logger.info(f"Aggregating dataframe by columns: {group_by_cols} with aggregations: {agg_dict} ")
    return df.groupBy(*group_by_cols).agg(agg_dict)

def calculate_totals(
        df: DataFrame,
        group_by_cols: List[str],
        sum_cols: List[str]
) -> DataFrame:
    logger.info(f"Calculating totals for columns: {sum_cols} grouped by: {group_by_cols}")
    agg_expr = [F.sum(F.col(c)).alias(f"total_{c}") for c in sum_cols]
    return df.groupBy(*group_by_cols).agg(*agg_expr)

def calculate_averages(
        df: DataFrame,
        group_by_cols: List[str],
        avg_cols: List[str]
) -> DataFrame:
    logger.info(f" Calculating averages for columns: {avg_cols} grouped by: {group_by_cols}")
    agg_expr = [F.avg(F.col(c)).alias(f"avg_{c}") for c in avg_cols]
    return df.groupBy(*group_by_cols).agg(*agg_expr)

def calculate_counts(
        df: DataFrame,
        group_by_cols: List[str],
        count_col: Optional[str] = None        
) -> DataFrame:
    logger.info(f"Calculating counts grouped by: {group_by_cols} on column: {count_col if count_col else 'all columns'}")
    if count_col:
        return df.groupBy(*group_by_cols).agg(F.count(count_col).alias(f"count"))
    else:
        return df.groupBy(*group_by_cols).count()
    
def calculate_distinct_counts(
        df: DataFrame,
        group_by_cols: List[str],
        distinct_cols: List[str]
) -> DataFrame:
    logger.info(f"Calculating distinct counts for columns: {distinct_cols} grouped by: {group_by_cols}")
    agg_expr = [F.countDistinct(F.col(c)).alias(f"distinct_count_{c}") for c in distinct_cols]
    return df.groupBy(*group_by_cols).agg(*agg_expr)

def calculate_summery_statistics(
        df: DataFrame,
        group_by_cols: List[str],
        stats_cols: List[str]
) -> DataFrame:
    logger.info(f"Calculating summary statistics for columns: {stats_cols} grouped by: {group_by_cols}")
    agg_expr = []
    for c in stats_cols:
        agg_expr.extend([
            F.min(F.col(c)).alias(f"min_{c}"),
            F.max(F.col(c)).alias(f"max_{c}"),
            F.avg(F.col(c)).alias(f"avg_{c}"),
            F.stddev(F.col(c)).alias(f"stddev_{c}")
        ])
    return df.groupBy(*group_by_cols).agg(*agg_expr)
