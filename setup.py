from setuptools import setup, find_packages

setup(
    name="customer_analytics",  # Replace with your package name
    version="3.0.1",
    packages=find_packages(),
    install_requires=[
        "boto3",
        "pyspark",
        "awswrangler"  # Useful for AWS data handling
    ],
    author="Azhagan",
    author_email="your.email@example.com",
    description="AWS Glue ETL pipeline for data processing",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/glue_etl_pipeline",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
)
