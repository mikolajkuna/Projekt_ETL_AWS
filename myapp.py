from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max
from pyspark.sql.window import Window

def main():
    # Tworzymy sesję Spark
    with SparkSession.builder.appName("MyApp").getOrCreate() as spark:
        # Wczytujemy dane z pliku Parquet
        df = spark.read.parquet("s3://668704778226-formatted-data")

        # Preprocessing: Usuwamy wiersze, w których 'status_id' jest NULL
        df = df.filter(df.status_id.isNotNull())

       # Grupujemy po prac_id i dacie, sprawdzamy minimalny i maksymalny status_id
        grouped = df.groupBy("prac_id", "data_czas").agg(
            min("status_id").alias("min_status"),
            max("status_id").alias("max_status")
        )
        
        # Sprawdzamy, którzy pracownicy nie mieli pełnej pary (wejście 1, wyjście 2)
        brak_pary = grouped.filter((col("min_status") != 1) | (col("max_status") != 2))

        # Generujemy wynik
        brak_pary.coalesce(1).write.json("s3://668704778226-result/results.json", mode='Overwrite')

if __name__ == "__main__":
    main()
    