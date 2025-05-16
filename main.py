from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

# Initialize Spark
spark = SparkSession.builder \
    .appName("PySpark Homework 3") \
    .getOrCreate()

print("✅ SparkSession initialized")

# Path to CSV files
DATA_PATH = "data/"

# Load CSV files
users_df = spark.read.csv(DATA_PATH + "users.csv", header=True, inferSchema=True)
purchases_df = spark.read.csv(DATA_PATH + "purchases.csv", header=True, inferSchema=True)
products_df = spark.read.csv(DATA_PATH + "products.csv", header=True, inferSchema=True)

# Show initial DataFrames
print("🧑 USERS")
users_df.show()

print("🛒 PURCHASES")
purchases_df.show()

print("📦 PRODUCTS")
products_df.show()

# 🔍 STEP 2: Drop rows with missing values
users_df = users_df.dropna()
purchases_df = purchases_df.dropna()
products_df = products_df.dropna()

print("✅ Missing values removed")

# Show cleaned DataFrames
print("🧑 USERS (cleaned)")
users_df.show()

print("🛒 PURCHASES (cleaned)")
purchases_df.show()

print("📦 PRODUCTS (cleaned)")
products_df.show()

# STEP 3: Total spending by product category
# Join purchases with products
purchases_with_products = purchases_df.join(products_df, on="product_id")

# Add column for total price
purchases_with_total = purchases_with_products.withColumn(
    "total", col("quantity") * col("price")
)

# Group by category and calculate total spending
total_by_category = purchases_with_total.groupBy("category").agg(
    _sum("total").alias("total_spent")
)

# Sort in descending order
total_by_category = total_by_category.orderBy(col("total_spent").desc())

print("📊 Total spending by product category:")
total_by_category.show()
