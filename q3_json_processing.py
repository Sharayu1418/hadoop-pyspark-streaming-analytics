"""
Q3 – JSON order processing with PySpark.

Part A:
  Explode nested products and compute per-product:
    total_quantity, total_revenue, num_orders.

Part B:
  Customer–product pivot table of quantities + total_items.
"""

import sys
from pyspark.sql import SparkSession, functions as F


def create_spark(app_name: str = "Q3_JSON_Processing") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def load_orders(spark: SparkSession, input_path: str):
    df = (
        spark.read
        .option("multiLine", False)
        .json(input_path)
    )
    return df


def compute_part_a(df):
    # One row per product line
    exploded = (
        df.withColumn("product", F.explode("products"))
          .select(
              "order_id",
              "customer_name",
              "order_date",
              F.col("product.product_id").alias("product_id"),
              F.col("product.name").alias("product_name"),
              F.col("product.price").alias("price"),
              F.col("product.quantity").alias("quantity"),
          )
    )

    # Revenue per line
    with_revenue = exploded.withColumn(
        "line_revenue", F.col("price") * F.col("quantity")
    )

    # Aggregate per product
    part_a = (
        with_revenue
        .groupBy("product_id", "product_name")
        .agg(
            F.sum("quantity").alias("total_quantity"),
            F.round(F.sum("line_revenue"), 2).alias("total_revenue"),
            F.countDistinct("order_id").alias("num_orders"),
        )
        .orderBy("product_id")
    )

    return part_a, exploded


def compute_part_b(exploded):
    # Exploded is one row per (order, product); reuse it for the pivot.
    customer_product = (
        exploded
        .groupBy("customer_name", "product_id")
        .agg(F.sum("quantity").alias("total_quantity"))
    )

    pivoted = (
        customer_product
        .groupBy("customer_name")
        .pivot("product_id")
        .sum("total_quantity")
    )

    # Replace nulls with 0 for missing products
    pivoted = pivoted.fillna(0)

    # Sum across all product columns to get total_items
    product_cols = [c for c in pivoted.columns if c != "customer_name"]
    total_expr = sum(F.col(c) for c in product_cols)

    part_b = pivoted.withColumn("total_items", total_expr).orderBy("customer_name")

    return part_b


def main(input_path: str):
    spark = create_spark()

    print(f"Reading orders from: {input_path}")
    df = load_orders(spark, input_path)

    print("\nSchema:")
    df.printSchema()

    print("\nRow count:")
    print(df.count())

    print("\n=== Part A: Per-product totals ===")
    part_a, exploded = compute_part_a(df)
    part_a.show(20, truncate=False)

    print("\n=== Part B: Customer–product pivot ===")
    part_b = compute_part_b(exploded)
    part_b.show(20, truncate=False)
    
    print("\n=== Saving Part A results to CSV ===")
    part_a.coalesce(1).write.mode("overwrite").csv("q3_part_a_results", header=True)
    
    print("\n=== Saving Part B results to CSV ===")
    part_b.coalesce(1).write.mode("overwrite").csv("q3_part_b_results", header=True)
    
    print("Results saved.")

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        input_path_arg = sys.argv[1]
    else:
        input_path_arg = "/home/jovyan/shared/midterm/q3_orders.json"

    main(input_path_arg)
