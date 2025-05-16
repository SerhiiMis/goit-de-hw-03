from pyspark.sql import SparkSession

# Ініціалізація Spark
spark = SparkSession.builder \
    .appName("PySpark Homework 3") \
    .getOrCreate()

print("✅ SparkSession initialized")

# Шлях до CSV
DATA_PATH = "data/"

# Завантаження CSV-файлів
users_df = spark.read.csv(DATA_PATH + "users.csv", header=True, inferSchema=True)
purchases_df = spark.read.csv(DATA_PATH + "purchases.csv", header=True, inferSchema=True)
products_df = spark.read.csv(DATA_PATH + "products.csv", header=True, inferSchema=True)

# Виведення перших рядків
print("🧑 USERS")
users_df.show()

print("🛒 PURCHASES")
purchases_df.show()

print("📦 PRODUCTS")
products_df.show()
