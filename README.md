# 🧠 PySpark Data Analysis Homework

## 📘 Task Overview

This project is a part of a course assignment focused on **data analysis using PySpark**.  
It includes loading CSV files, cleaning data, joining datasets, and performing analytical queries using PySpark's DataFrame API.

---

## 📂 Input Datasets

The project uses three CSV files:

- `users.csv` – user info: `user_id`, `name`, `age`, `email`
- `purchases.csv` – purchase data: `purchase_id`, `user_id`, `product_id`, `date`, `quantity`
- `products.csv` – product info: `product_id`, `product_name`, `category`, `price`

All files are expected to be in the `/data` folder.

---

## 🧩 Steps Completed

1. Load CSV files into PySpark DataFrames
2. Remove rows with missing values
3. Calculate total purchase sum by product category
4. Calculate purchase sum by product category for users aged 18–25
5. Calculate spending share (%) by category for 18–25 age group
6. Select top 3 categories by percentage share for age group 18–25

---

## ⚙️ How to Run

1. Activate your virtual environment:
   ```bash
   source pyspark-env/bin/activate
   ```
2. Run the script:
   `bash
    Copy
    Edit
    python main.py
    `
   Make sure Spark is installed and you’re using WSL or Linux-based environment.

---

## 🛠 Technologies Used

Python 3.12+

PySpark 3.5.5

Apache Spark

WSL (Ubuntu 24.04)
