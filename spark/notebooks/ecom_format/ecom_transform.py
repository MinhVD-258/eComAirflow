# README:
# SPARK_APPLICATION_ARGS contains stock-market/AAPL/prices.json
# SPARK_APPLICATION_ARGS will be passed to the Spark application as an argument -e when running the Spark application from Airflow
# - Sometimes the script can stay stuck after "Passing arguments..."
# - Sometimes the script can stay stuck after "Successfully stopped SparkContext"
# - Sometimes the script can show "WARN TaskSchedulerImpl: Initial job has not accepted any resources; check your cluster UI to ensure that workers are registered and have sufficient resources"
# The easiest way to solve that is to restart your Airflow instance
# astro dev kill && astro dev start
# Also, make sure you allocated at least 8gb of RAM to Docker Desktop
# Go to Docker Desktop -> Preferences -> Resources -> Advanced -> Memory

# Import the SparkSession module
from pyspark.sql import SparkSession
from pyspark import SparkContext
import pyspark.sql.functions as f
from pyspark.sql.types import DateType

import os
import sys

if __name__ == '__main__':

    def app():
        # Create a SparkSession
        spark = SparkSession.builder.appName("FormatEcom") \
            .config("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "minio")) \
            .config("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")) \
            .config("fs.s3a.endpoint", os.getenv("ENDPOINT", "http://host.docker.internal:9000")) \
            .config("fs.s3a.connection.ssl.enabled", "false") \
            .config("fs.s3a.path.style.access", "true") \
            .config("fs.s3a.attempts.maximum", "1") \
            .config("fs.s3a.connection.establish.timeout", "500000") \
            .config("fs.s3a.connection.timeout", "1000000") \
            .getOrCreate()

        # Read a JSON file from an MinIO bucket using the access key, secret key, 
        # and endpoint configured above
        df = spark.read.option("header", "true") \
            .json(f"s3a://{os.getenv('SPARK_APPLICATION_ARGS')}/raw.csv")

        # Change data types/schema
        df_dtype = df.withColumn('event_time', f.to_timestamp('event_time', 'yyyy-MM-dd HH:mm:ss z')) \
                     .withColumn('event_date', f.day('event_time')) \
                     .withColumn('price', df['price'].cast('float'))
        
        # Split category column
        df_split = df_dtype.withColumn('cate_gr', f.split(df_dtype['category_code'],r'\.'))
        df_split = df_split.select('event_time', 'event_type', 'product_id','brand','price','user_id','user_session','category_code',
                     *[f.get(df_split.cate_gr,i).alias(f'cate_{i+1}') for i in range(0, 4)])
        

        # Store in Minio
        df_split.write \
            .mode("overwrite") \
            .option("header", "true") \
            .option("delimiter", ",") \
            .csv(f"s3a://{os.getenv('SPARK_APPLICATION_ARGS')}/formatted.csv")

    app()
    os.system('kill %d' % os.getpid())