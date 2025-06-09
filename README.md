# 🛠️ ETL Pipeline in Python

## 📌 Overview

This project demonstrates a simple yet complete **ETL (Extract, Transform, Load)** pipeline using Python in a Google Colab Notebook.  
It automatically downloads CSV and Excel files from the internet, processes the data, and prepares it for further analysis or loading into databases.

The goal is to practice automation and reproducibility in data workflows. Each time the script is run, fresh data is pulled directly from the source to ensure up-to-date results.

---

## 📂 Dataset Used

- **CSV File:** [here](https://assets.swisscoding.edu.vn/company_course/work_experience.csv)
- **Excel File:** [here](https://assets.swisscoding.edu.vn/company_course/enrollies_education.xlsx)

📥 The files are **not stored locally**. Instead, they are **downloaded programmatically** at runtime using direct URLs.

---

## 🔄 ETL Process Description

### 1. **Extract**
- Files (CSV and Excel) are downloaded via the `requests` library.
- The download function ensures files are reloaded from the source each time the notebook runs.
- Files are stored temporarily in a `./data/` directory.

### 2. **Transform**
- Data is loaded into `pandas` DataFrames.
- Cleaning steps (e.g., handling nulls, modified,...) are applied depending on the structure of each file.
- The transformation logic is written modularly to support reusable processing.

### 3. **Load**
- Transformed data can be printed, saved and loading into a database named warehouse.db

---

## 🧠 Decisions & Justifications

- **Why download programmatically?**  
  Ensures **reproducibility** and **automation**, which are critical in modern data workflows.

- **Why `requests` + `pandas`?**  
  Lightweight and well-supported in Colab; `requests` is reliable for HTTP download, and `pandas` is ideal for data manipulation.

- **Why modular functions?**  
  Reusability and better readability, important in production-grade ETL pipelines.


