from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max
from pyspark.sql.window import Window

def main():
    # Creating a Spark session
    with SparkSession.builder.appName("MyApp").getOrCreate() as spark:
        # Loading data from a Parquet file
        df = spark.read.parquet("s3://668704778226-formatted-data")

        # Preprocessing: Removing rows where 'status_id' is NULL
        df = df.filter(df.status_id.isNotNull())

        # Grouping by 'prac_id' and 'data_czas', checking for the minimum and maximum 'status_id'
        grouped = df.groupBy("prac_id", "data_czas").agg(
            min("status_id").alias("min_status"),
            max("status_id").alias("max_status")
        )
        
        # Checking which employees did not have a full pair (entry 1, exit 2)
        brak_pary = grouped.filter((col("min_status") != 1) | (col("max_status") != 2))

        # Generating the output
        brak_pary.coalesce(1).write.json("s3://668704778226-result/results.json", mode='Overwrite')

if __name__ == "__main__":
    main()
    