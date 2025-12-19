# PySpark Transformations for AWS Glue ETL

This repository contains **Python (PySpark) transformation code** designed to run as **AWS Glue ETL jobs**.

The purpose of this project is to **transform raw and semi-processed data** into **analytics-ready datasets** as part of a data lake pipeline.

---

## 📌 What this project is for

- Execute **ETL transformations inside AWS Glue**
- Process data stored in **Amazon S3**
- Apply **business logic and data cleaning**
- Generate curated outputs for **analytics and reporting**
- Package and deploy transformation code as a **reusable artifact**

This code is **not a standalone application** — it is executed by **AWS Glue jobs**.

---

## 🧱 Key Responsibilities

- Read input data from S3 (Bronze / Silver layers)
- Perform:
  - Data cleaning and preprocessing
  - Feature engineering
  - Aggregations and business logic
- Write transformed output back to S3 (Silver / Gold layers)
- Register outputs in **AWS Glue Data Catalog**

---

##  Executables

```pip install wheel setuptools```

Build 
----------------
```python setup.py bdist_wheel```

Copy wheel file to S3
-----------------------

```aws s3 cp dist/customer_analytics-2.1.0-py3-none-any.whl s3://customer-analytics-oct2025-az/code/customer_analytics/```

Copy glue script files 
-----------------------

```aws s3 cp glue_upload_script s3://customer-analytics-oct2025-az/code/customer_analytics/ --recursive```

Git Commands :
--------------------
```git clone ---url```

```git checkout -b feature-1001```

```git remote add origin <path to the github repo>.git ``` -- adding the remote location

```git add .```

```git commit -m " desc"```

```git push origin main```
