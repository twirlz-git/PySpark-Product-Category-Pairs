# PySpark-Product-Category-Pairs

In the PySpark application, data frames (pyspark.sql.DataFrame) define products, categories, and their relationships. 
Each product can correspond to several categories or none at all. Each category can correspond to several products or none at all. 

 I wrote a method `products_categories.py` in PySpark that returns all pairs of “Product Name - Category Name” and the names of all products that do not have categories in a single dataframe. 
 
 Also wrote tests for my method `test_products_categories.py`
