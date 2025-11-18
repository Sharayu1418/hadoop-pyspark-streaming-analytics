#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Q2 – Spark DataFrames: Basic E-commerce Analysis
Big Data CS-GY 6513 Fall 2025

Part A: Per-customer aggregations (num_orders, total_spent, avg_order_value, top_product)
Part B: Window functions (order ranking, days between orders, first/last product)
"""

import sys
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def create_spark_session(app_name: str = "Q2_Ecommerce_Analysis") -> SparkSession:
    """Initialize Spark session with optimized settings for limited resources."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def load_and_clean_data(spark: SparkSession, input_path: str):
    """
    Load and clean the Online Retail dataset.
    
    Cleaning steps:
    - Remove null CustomerIDs
    - Remove negative Quantity and UnitPrice
    - Exclude cancellations (InvoiceNo starting with 'C')
    - Parse InvoiceDate to timestamp
    """
    print(f"\nLoading dataset from: {input_path}")
    
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(input_path)
    )
    
    initial_count = df.count()
    print(f"Initial record count: {initial_count}")
    
    # Clean and filter
    df = (
        df
        .filter(F.col("CustomerID").isNotNull())
        .filter(F.col("Quantity") > 0)
        .filter(F.col("UnitPrice") > 0)
        .filter("InvoiceNo NOT LIKE 'C%'") # <-- HERE IS THE FIX
    )
    
    # Parse date with robust error handling
    df = df.withColumn(
        "InvoiceTimestamp",
        F.expr("try_to_timestamp(InvoiceDate, 'M/d/yyyy H:mm')")
    ).filter(F.col("InvoiceTimestamp").isNotNull())
    
    filtered_count = df.count()
    print(f"Record count after filtering: {filtered_count}")
    print(f"Removed {initial_count - filtered_count} records ({(initial_count - filtered_count) / initial_count * 100:.1f}%)")
    
    # Cache as it will be reused
    df.cache()
    
    return df


def compute_part_a(df):
    """
    Part A: Basic Aggregations per Customer
    
    Returns: DataFrame with columns:
        - CustomerID
        - num_orders
        - total_spent
        - avg_order_value
        - top_product
    """
    print("\n" + "="*70)
    print("PART A: BASIC AGGREGATIONS")
    print("="*70)
    
    # Calculate line amount
    df_with_amount = df.withColumn(
        "LineAmount", F.col("Quantity") * F.col("UnitPrice")
    )
    
    # Aggregate to order level first
    orders = (
        df_with_amount
        .groupBy("CustomerID", "InvoiceNo")
        .agg(F.sum("LineAmount").alias("order_value"))
    )
    
    # Customer-level aggregations
    cust_agg = (
        orders
        .groupBy("CustomerID")
        .agg(
            F.countDistinct("InvoiceNo").alias("num_orders"),
            F.round(F.sum("order_value"), 2).alias("total_spent")
        )
        .withColumn(
            "avg_order_value",
            F.round(F.col("total_spent") / F.col("num_orders"), 2)
        )
    )
    
    # Find top product per customer
    # If frequency tie, select product with highest total amount spent
    prod_agg = (
        df_with_amount
        .groupBy("CustomerID", "StockCode")
        .agg(
            F.countDistinct("InvoiceNo").alias("order_frequency"),
            F.sum("LineAmount").alias("product_total_spent")
        )
    )
    
    # Window to rank products by frequency, then by spend
    prod_window = Window.partitionBy("CustomerID").orderBy(
        F.col("order_frequency").desc(),
        F.col("product_total_spent").desc()
    )
    
    
    top_products = (
        prod_agg
        .withColumn("rank", F.rank().over(prod_window))
        .filter(F.col("rank") == 1)
        .groupBy("CustomerID")
        .agg(F.first("StockCode").alias("top_product"))
    )
    
    # Join aggregations with top product
    result = cust_agg.join(top_products, on="CustomerID", how="left")
    
    return result.orderBy("CustomerID")


def compute_part_b(df):
    """
    Part B: Window Functions
    
    Returns: DataFrame with columns:
        - CustomerID
        - InvoiceNo
        - InvoiceTimestamp
        - order_number (rank by date)
        - days_between_orders (days since previous order)
        - order_value
        - first_product (first product ever purchased by customer)
        - last_product (last product purchased by customer)
    """
    print("\n" + "="*70)
    print("PART B: WINDOW FUNCTIONS")
    print("="*70)
    
    # Calculate line amounts
    df_with_amount = df.withColumn(
        "LineAmount", F.col("Quantity") * F.col("UnitPrice")
    )
    
    # Window for first/last product (unbounded to see all orders)
    unbounded_window = (
        Window.partitionBy("CustomerID")
        .orderBy("InvoiceTimestamp")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    
    # Add first and last product to each line item
    df_with_products = (
        df_with_amount
        .withColumn("first_product", F.first("StockCode").over(unbounded_window))
        .withColumn("last_product", F.last("StockCode").over(unbounded_window))
    )
    
    # Aggregate to order level (one row per invoice)
    orders = (
        df_with_products
        .groupBy("CustomerID", "InvoiceNo", "InvoiceTimestamp", "first_product", "last_product")
        .agg(F.round(F.sum("LineAmount"), 2).alias("order_value"))
    )
    
    # Window for ranking orders chronologically
    order_window = Window.partitionBy("CustomerID").orderBy("InvoiceTimestamp")
    
    # Apply window functions
    result = (
        orders
        .withColumn("order_number", F.rank().over(order_window))
        .withColumn("prev_order_date", F.lag("InvoiceTimestamp").over(order_window))
        .withColumn(
            "days_between_orders",
            F.datediff(F.col("InvoiceTimestamp"), F.col("prev_order_date"))
        )
        .drop("prev_order_date")
    )
    
    return result.orderBy("CustomerID", "order_number")


def display_results(part_a, part_b):
    """Display formatted results for both parts."""
    
    print("\n" + "="*70)
    print("PART A RESULTS: Customer Aggregations")
    print("="*70)
    print("CustomerId | num_orders | total_spent | avg_order_value | top_product")
    print("-" * 70)
    part_a.show(20, truncate=False)
    
    print("\n" + "="*70)
    print("PART B RESULTS: Window Functions (Combined Output)")
    print("="*70)
    print("Shows ranking, days between orders, and first/last product together")
    print("-" * 110)
    part_b.select(
        "CustomerID",
        "InvoiceNo",
        "InvoiceTimestamp",
        "order_number",
        "days_between_orders",
        "order_value",
        "first_product",
        "last_product"
    ).show(30, truncate=False)
    
    # Summary statistics
    print("\n" + "="*70)
    print("SUMMARY STATISTICS")
    print("="*70)
    
    total_customers = part_a.count()
    total_orders = part_b.count()
    orders_per_customer = part_b.groupBy("CustomerID").agg(
        F.max("order_number").alias("max_order_num")
    )
    single_order_customers = orders_per_customer.filter(
        F.col("max_order_num") == 1
    ).count()
    multi_order_customers = orders_per_customer.filter(
        F.col("max_order_num") > 1
    ).count()
    
    
    avg_days = (
        part_b
        .filter(F.col("days_between_orders").isNotNull())
        .agg(F.avg("days_between_orders").alias("avg_days"))
        .collect()[0]["avg_days"]
    )
    
    print(f"Total unique customers: {total_customers}")
    print(f"Total orders processed: {total_orders}")
    print(f"Customers with single order: {single_order_customers}")
    print(f"Customers with multiple orders: {multi_order_customers}")
    print(f"Average orders per customer: {total_orders / total_customers:.2f}")
    print(f"Average days between consecutive orders: {avg_days:.2f}")


def save_results(part_a, part_b, output_dir: str = "."):
    """Optional: Save results to CSV files."""
    print(f"\nSaving results to {output_dir}/...")
    
    part_a.coalesce(1).write.mode("overwrite") \
        .option("header", True).csv(f"{output_dir}/q2_partA_output")
    
    part_b.coalesce(1).write.mode("overwrite") \
        .option("header", True).csv(f"{output_dir}/q2_partB_output")
    
    print("Results saved successfully")


def main(input_path: str, save_output: bool = True):
    """Main execution function."""
    
    # Initialize Spark
    spark = create_spark_session()
    print("Spark session initialized successfully")
    
    try:
        # Load and clean data
        df = load_and_clean_data(spark, input_path)
        
        # Compute Part A
        part_a = compute_part_a(df)
        
        # Compute Part B
        part_b = compute_part_b(df)
        
        # Display results
        display_results(part_a, part_b)
        
        # Optional: Save results
        if save_output:
            save_results(part_a, part_b)
        
        # Cleanup
        df.unpersist()
        print("\nDataFrame unpersisted successfully")
        
    finally:
        spark.stop()
        print("Spark session stopped successfully")


if __name__ == "__main__":
    
    default_path = "/home/jovyan/shared/midterm/Online Retail.csv"
    
    # Allow command-line argument for input path
    input_file = sys.argv[1] if len(sys.argv) > 1 else default_path
    
    # Run analysis
    main(input_file, save_output=True)