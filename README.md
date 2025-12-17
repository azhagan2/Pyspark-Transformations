pip install wheel setuptools


Build 
----------------
python setup.py bdist_wheel


Copy wheel file to S3
-----------------------

aws s3 cp dist/customer_analytics-2.1.0-py3-none-any.whl s3://customer-analytics-oct2025-az/code/customer_analytics/



Copy glue script files 
-----------------------

aws s3 cp glue_upload_script s3://customer-analytics-oct2025-az/code/customer_analytics/ --recursive



Git Commands :
--------------------
git clone ---url

git checkout -b feature-1001   






git add .

git commit -m " desc"
git push