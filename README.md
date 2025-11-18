Big Data Analytics Project – Hadoop, PySpark, and Streaming  
Author: Sharayu Rasal

---

## Overview

This project showcases end‑to‑end **big data engineering and analytics** using Hadoop MapReduce, PySpark DataFrames, JSON processing, and Spark Structured Streaming. It covers batch and near real‑time pipelines for text analytics, e‑commerce transaction analysis, nested JSON orders, and sensor data streams.

**Tech stack:** Python 3, Hadoop Streaming, Apache Spark (PySpark, SQL, Structured Streaming), Jupyter Notebook.

---

## Repository Structure

- `q1_mapper.py` – Hadoop Streaming mapper for word count + statistics  
- `q1_reducer.py` – Hadoop Streaming reducer for aggregated statistics  
- `q1_output.txt` – Sample output for Q1  

- `q2_ecommerce_analysis.py` – Main PySpark script for Q2  
- `q2_ecommerce_analysis.ipynb` – Notebook wrapper / runner for Q2  
- `q2_part_a_customer_summary.csv` – Saved Part A results (per-customer aggregations)  
- `q2_part_b_customer_orders_window.csv` – Saved Part B results (window functions)  

- `q3_json_processing.py` – Main PySpark script for Q3  
- `q3_json_processing.ipynb` – Notebook wrapper / runner for Q3  
- `q3_part_a_product_statistics.csv` – Saved Part A results (per-product statistics)  
- `q3_part_b_customer_product_pivot.csv` – Saved Part B results (customer–product pivot)  

- `q4_streaming.ipynb` – Complete Spark Structured Streaming solution for Q4  

- `bonus.py` – PySpark script for bonus question (UDF + joins)  
- `bonus.ipynb` – Notebook wrapper / runner for bonus  
---

## Prerequisites

- **Python 3.x**
- **Apache Spark** with PySpark available on the `PATH` (for Q2, Q3, Q4, Bonus)
- **Hadoop** with the Hadoop Streaming JAR (for Q1)  
- **Jupyter Notebook / JupyterLab** (to run the `.ipynb` files, if desired)

Default input paths in the scripts assume a Jupyter/Spark lab environment, e.g.:

- Online Retail CSV: `/home/jovyan/shared/midterm/Online Retail.csv`  
- Q3 JSON orders: `/home/jovyan/shared/midterm/q3_orders.json`  
- Q4 sensor data JSON: `/home/jovyan/shared/midterm/q4_sensor_data.json`

You can override these by passing arguments on the command line (see details below).

---

## Module 1 – Hadoop MapReduce: Word Count with Statistics

**Goal:** Implement a Hadoop Streaming job that performs a classic **word count**, plus additional statistics:

- Top N most frequent words  
- Distribution of word lengths  
- Total words, unique words, and average word length

### Files

- `q1_mapper.py`  
- `q1_reducer.py`  

### How It Works

**Mapper (`q1_mapper.py`):**

- Converts all text to lowercase.  
- Uses a regex to retain only alphabetic characters (`[a-z]+`).  
- Filters out stop words: `{"the", "is", "an", "a", "are"}`.  
- Emits three types of key–value pairs:
  - `word_count:<word>\t1` – for word frequency  
  - `word_length:<length>\t1` – for length distribution  
  - `total_words:all\t1` – for global total word count  

**Reducer (`q1_reducer.py`):**

- Aggregates counts separately for:
  - `word_count:*` (per-word frequency)  
  - `word_length:*` (length distribution)  
  - `total_words:all` (global total)  
- Outputs:
  - Top 20 most frequent words  
  - Word-length distribution  
  - Total words, number of unique words, and average word length  
- Handles malformed lines defensively (logs issues to `stderr`).

### How to Run (Hadoop Streaming)

Example command (adjust paths as needed):

```bash
hadoop jar /path/to/hadoop-streaming.jar \
  -input /path/to/plato.txt \
  -output /path/to/output \
  -mapper q1_mapper.py \
  -reducer q1_reducer.py \
  -file q1_mapper.py \
  -file q1_reducer.py
```

### Key Assumptions & Design Choices

- Only alphabetic tokens are treated as words.  
- The stop-word list is intentionally small (five common words).  
- Empty lines and malformed mapper output are skipped to keep the reducer robust.  
- Using **prefixed keys** (`word_count:`, `word_length:`, `total_words:`) allows multiple aggregations within a single MapReduce job.

---

## Module 2 – PySpark DataFrames: E‑commerce Analysis

**Goal:** Use PySpark DataFrames and window functions to analyze an **Online Retail** transactional dataset.

- **Part A:** Per-customer metrics (number of orders, total spent, average order value, top product).  
- **Part B:** Window functions to rank orders, compute days between orders, and track first/last product purchased.

### Files

- `q2_ecommerce_analysis.py` – Main script  
- `q2_ecommerce_analysis.ipynb` – Notebook wrapper  
- `q2_part_a_customer_summary.csv` – Part A results  
- `q2_part_b_customer_orders_window.csv` – Part B results  

### How to Run

From the command line (standard Spark):

```bash
spark-submit q2_ecommerce_analysis.py /path/to/Online\ Retail.csv
```

If no argument is provided, the script defaults to:

```text
/home/jovyan/shared/midterm/Online Retail.csv
```

From Jupyter (for testing):

```python
%run q2_ecommerce_analysis.py "/path/to/Online Retail.csv"
```

### Data Cleaning & Assumptions

- Requires columns: `CustomerID`, `InvoiceNo`, `Quantity`, `UnitPrice`, `InvoiceDate`.  
- Rows are filtered if:
  - `CustomerID` is `NULL`, or  
  - `Quantity <= 0`, or  
  - `UnitPrice <= 0`, or  
  - `InvoiceNo` indicates cancellations (`InvoiceNo LIKE 'C%'`).  
- `InvoiceDate` is parsed using `try_to_timestamp(InvoiceDate, 'M/d/yyyy H:mm')`.  
- `spark.sql.shuffle.partitions` is set to `4` to respect resource constraints.

### Part A – Per-Customer Aggregations

- Compute line amounts: `LineAmount = Quantity * UnitPrice`.  
- Aggregate to **order level** (per `CustomerID` + `InvoiceNo`), then to **customer level**:
  - `num_orders` – count of distinct invoices  
  - `total_spent` – sum of order values  
  - `avg_order_value` – `total_spent / num_orders`  
- Determine **top product per customer** using:
  - Primary ranking: order frequency (number of distinct invoices containing the product).  
  - Tie-breaker: total amount spent on that product.

### Part B – Window Functions

- Aggregate to one row per order with `order_value`.  
- Use window partitions by `CustomerID` ordered by `InvoiceTimestamp` to compute:
  - `order_number` – rank of each order per customer.  
  - `days_between_orders` – date difference from previous order.  
  - `first_product` and `last_product` – first and last products a customer ever purchased (via unbounded windows).  
- Additional summary statistics:
  - Total unique customers and total orders.  
  - Count of single-order vs multi-order customers.  
  - Average orders per customer.  
  - Average days between consecutive orders.

### Outputs

- **Console:** Formatted tables for Part A and Part B plus summary statistics.  
- **CSV files (by default):**
  - `q2_partA_output/` – Per-customer aggregations.  
  - `q2_partB_output/` – Window-function results (per order).

---

## Module 3 – PySpark JSON Processing

**Goal:** Work with **nested JSON** order data:

- **Part A:** Explode nested `products` arrays and compute per-product statistics.  
- **Part B:** Build a customer–product pivot table with total-items summary.

### Files

- `q3_json_processing.py` – Main script  
- `q3_json_processing.ipynb` – Notebook wrapper  
- `q3_part_a_product_statistics.csv` – Part A results  
- `q3_part_b_customer_product_pivot.csv` – Part B results  

### How to Run

From the command line:

```bash
spark-submit q3_json_processing.py /path/to/q3_orders.json
```

From Jupyter:

```python
%run q3_json_processing.py "/path/to/q3_orders.json"
```

If no argument is provided, the default input path is:

```text
/home/jovyan/shared/midterm/q3_orders.json
```

### Data Assumptions

- Input is **JSON Lines** (one JSON object per line) → `multiLine=False`.  
- Each row represents one order with a required `products` array.  
- Each product entry provides: `product_id`, `name`, `price`, `quantity`.  
- `price` and `quantity` are valid numeric fields.  
- `customer_name` uniquely identifies customers for the pivot.

### Part A – Per-Product Statistics

- Explode the `products` array with `F.explode("products")` to obtain one row per product line.  
- Compute `line_revenue = price * quantity`.  
- Group by `product_id` and `product_name` to compute:
  - `total_quantity` – total units sold  
  - `total_revenue` – total revenue (rounded to 2 decimals)  
  - `num_orders` – number of distinct orders containing the product  
- Sort by `product_id` and save results to `q3_part_a_results/`.

### Part B – Customer–Product Pivot

- Start from exploded rows (one row per order–product pair).  
- Aggregate quantities to get per-customer per-product totals.  
- Pivot on `product_id` to create a wide table where:
  - Rows = `customer_name`  
  - Columns = product IDs  
  - Values = total quantity purchased  
- Replace `NULL` values with `0` and add `total_items` column as the row-wise sum of all product columns.  
- Save the pivoted table to `q3_part_b_results/`.

---

## Module 4 – Spark Structured Streaming

**Goal:** Build a **Spark Structured Streaming** pipeline in a Jupyter notebook to process sensor data using:

- One main input stream from a watched directory.  
- Multiple streaming queries (e.g., aggregated statistics with different windows).

### Files

- `q4_streaming.ipynb` – Complete solution with all code and outputs.

### How to Run (Notebook-Style)

This question is designed entirely around running `q4_streaming.ipynb` in Jupyter:

1. **Open** `q4_streaming.ipynb` in Jupyter.  
2. **Run Cell 1:**  
   - Creates a fresh `SparkSession`.  
   - Configures `spark.sql.shuffle.partitions = 4` for resource limits.  
   - Starts all three streaming queries (Part A, Part B-tumbling window, Part B-sliding window).  
3. **Run Cell 2:**  
   - A `%%bash` cell that copies `q4_sensor_data.json` into the watched input directory (e.g. `/home/jovyan/sensor_input/`).  
   - Triggers the streaming queries to process the new data.  
4. **Observe results:**  
   - Streaming outputs update directly in the notebook cell where the queries were started.  
5. **Run Cell 3:**  
   - Stops all active streaming queries cleanly.

### Assumptions & Design Considerations

- Sensor data resides in `/home/jovyan/shared/midterm/q4_sensor_data.json`.  
- The streaming job monitors `/home/jovyan/sensor_input/` for new JSON files.  
- `awaitAnyTermination()` is **not** used, to avoid blocking the notebook UI. Instead, long-running streams are explicitly stopped in a separate cell.  
- Running three concurrent streaming queries can be resource-intensive, so shuffle partitions are reduced to stabilize performance.

---

## Bonus Module – Advanced DataFrame Operations (UDFs & Joins)

**Goal:** Demonstrate more advanced DataFrame operations on the Online Retail dataset:

1. Use a **UDF** to categorize orders as *Small*, *Medium*, or *Large* based on order value.  
2. Compare **inner** vs **left joins** between all customers and “frequent customers”.

### Files

- `bonus.py` – Main script  
- `bonus.ipynb` – Notebook wrapper / runner  

### How to Run

From the command line:

```bash
spark-submit bonus.py /path/to/Online\ Retail.csv
```

From Jupyter:

```python
%run bonus.py "/path/to/Online Retail.csv"
```

If no argument is provided, the script defaults to:

```text
/home/jovyan/shared/midterm/Online Retail.csv
```

### Data Cleaning & UDF Logic

- Cleans the Online Retail dataset similarly to Q2:
  - Drops rows with null `CustomerID`.  
  - Filters out `Quantity <= 0` and `UnitPrice <= 0`.  
  - Parses `InvoiceDate` into `InvoiceTimestamp` via `try_to_timestamp`.  
- Aggregates to **order-level** (`CustomerID`, `InvoiceNo`) with `order_value`.  
- Applies `order_size_udf(order_value)`:
  - `< 50` → `"Small"`  
  - `50–200` → `"Medium"`  
  - `> 200` → `"Large"`  
  - `None` → `"Unknown"`

### Join Demonstration

- Builds a **per-customer summary**:
  - `num_orders` (number of distinct invoices)  
  - `total_spent` (rounded sum of order values)  
- Derives a `frequent` customers DataFrame:
  - Customers with `num_orders >= 5`, tagged with `segment = "frequent"`.  
- Shows:
  - **Inner join** – only customers who appear in both summary and frequent sets.  
  - **Left join** – all customers, with `segment` populated only for frequent ones.  
- Mentions (in code comments) the option to **coalesce** partitions before writing results to disk, to respect resource constraints.

---

## Notes for GitHub Viewers

- This repository is structured as a complete, self‑contained big data analytics project, with scripts, notebooks, and CSV outputs.  
- Input datasets are **not** included here due to size and licensing; you will need to provide your own copies and adjust paths as necessary.  
- For quick exploration, start with the notebooks (`*.ipynb`), which show both the code and example outputs for each module.


