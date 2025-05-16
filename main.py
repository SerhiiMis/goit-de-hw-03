from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum
from pyspark.sql.functions import round


spark = SparkSession.builder \
    .appName("PySpark Homework 3") \
    .getOrCreate()

print("✅ SparkSession initialized")

DATA_PATH = "data/"

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

# STEP 4: Spending by category for users aged 18–25
# Filter users in age group 18–25
young_users = users_df.filter((col("age") >= 18) & (col("age") <= 25))

# Join users with purchases
young_purchases = young_users.join(purchases_df, on="user_id")

# Join with products
young_purchases_with_products = young_purchases.join(products_df, on="product_id")

# Calculate total per purchase
young_purchases_with_total = young_purchases_with_products.withColumn(
    "total", col("quantity") * col("price")
)

# Group by category and sum totals
young_total_by_category = young_purchases_with_total.groupBy("category").agg(
    _sum("total").alias("total_spent")
)

# Sort by total spent descending
young_total_by_category = young_total_by_category.orderBy(col("total_spent").desc())

print("📊 Spending by category (age 18–25):")
young_total_by_category.show()

# STEP 5: Percentage share of spending by category (age 18–25)

# Get total sum across all categories for age 18–25
total_spent_all = young_total_by_category.agg(
    _sum("total_spent").alias("total_all")
).collect()[0]["total_all"]

# Add percentage column to each category
young_share_by_category = young_total_by_category.withColumn(
    "percentage",
    round((col("total_spent") / total_spent_all) * 100, 2)
)

print("📊 Percentage share by category (age 18–25):")
young_share_by_category.show()

# STEP 6: Top 3 categories by percentage (age 18–25)
top3_categories = young_share_by_category.limit(3)

print("🥇 Top 3 categories by percentage (age 18–25):")
top3_categories.show()
