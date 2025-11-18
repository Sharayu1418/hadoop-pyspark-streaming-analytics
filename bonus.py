"""
BONUS – Advanced DataFrame operations on the Online Retail dataset.

1) UDF:
   Categorize order_value as Small / Medium / Large.

2) Joins:
   Inner join vs left join between:
     - per-customer summary
     - a "frequent customers" DataFrame (customers with >=5 orders).
"""

import sys
from pyspark.sql import SparkSession, functions as F, types as T


def create_spark(app_name: str = "Bonus_Advanced_DF") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    # Keep shuffles small for this cluster.
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    return spark


def load_and_clean_data(spark: SparkSession, input_path: str):
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(input_path)
    )

    df = (
        df
        .filter(F.col("CustomerID").isNotNull())
        .filter(F.col("Quantity") > 0)
        .filter(F.col("UnitPrice") > 0)
    )

    df = df.withColumn(
        "InvoiceTimestamp",
        F.expr("try_to_timestamp(InvoiceDate, 'M/d/yyyy H:mm')")
    ).filter(F.col("InvoiceTimestamp").isNotNull())

    return df


def categorize_amount(amount: float) -> str:
    if amount is None:
        return "Unknown"
    if amount < 50:
        return "Small"
    if amount <= 200:
        return "Medium"
    return "Large"


order_size_udf = F.udf(categorize_amount, T.StringType())


def build_order_table(df):
    # Order-level table with one row per invoice.
    line_amount = df.withColumn(
        "LineAmount", F.col("Quantity") * F.col("UnitPrice")
    )

    orders = (
        line_amount
        .groupBy("CustomerID", "InvoiceNo")
        .agg(F.sum("LineAmount").alias("order_value"))
    )

    # Apply UDF to categorize order_value.
    orders = orders.withColumn(
        "order_size",
        order_size_udf(F.col("order_value"))
    )

    return orders


def build_customer_summary(orders):
    # Simple per-customer stats reused for join demo.
    summary = (
        orders
        .groupBy("CustomerID")
        .agg(
            F.countDistinct("InvoiceNo").alias("num_orders"),
            F.round(F.sum("order_value"), 2).alias("total_spent"),
        )
        .orderBy("CustomerID")
    )
    return summary


def build_frequent_customers(summary, min_orders: int = 5):
    # "Dimension" DataFrame with only frequent customers.
    frequent = (
        summary
        .filter(F.col("num_orders") >= min_orders)
        .select("CustomerID")
        .withColumn("segment", F.lit("frequent"))
    )
    return frequent


def main(input_path: str):
    spark = create_spark()

    print(f"Reading Online Retail data from: {input_path}")
    df = load_and_clean_data(spark, input_path)
    print("Row count after cleaning:")
    print(df.count())

    # ---------- Part 1: UDF ----------
    print("\n=== Part 1: UDF for order size ===")
    orders = build_order_table(df)
    orders.show(20, truncate=False)

    # ---------- Part 2: Joins ----------
    print("\n=== Part 2: Joins (inner vs left) ===")
    summary = build_customer_summary(orders)
    frequent = build_frequent_customers(summary, min_orders=5)

    print("\nCustomer summary (sample):")
    summary.show(10, truncate=False)

    print("\nFrequent customers (num_orders >= 5):")
    frequent.show(10, truncate=False)

    print("\nInner join (only customers that appear in BOTH tables):")
    inner_join = summary.join(frequent, on="CustomerID", how="inner")
    inner_join.show(10, truncate=False)

    print("\nLeft join (all customers, segment only for frequent ones):")
    left_join = summary.join(frequent, on="CustomerID", how="left")
    left_join.show(10, truncate=False)

    # Example of reducing partitions before writing out (commented out):
    # left_join.coalesce(4).write.mode("overwrite").parquet("bonus_join_output")

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        input_path_arg = sys.argv[1]
    else:
        input_path_arg = "/home/jovyan/shared/midterm/Online Retail.csv"

    main(input_path_arg)
