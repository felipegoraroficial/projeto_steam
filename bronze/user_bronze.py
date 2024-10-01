from pyspark.sql import SparkSession
from pyspark.sql.functions import  col, to_json, regexp_extract, to_date, row_number,input_file_name
from pyspark.sql.window import Window
import os

def bronze_user_steam():

    spark = SparkSession.builder \
        .appName("BronzeStep") \
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.0") \
        .getOrCreate()

    path = "/home/fececa/airflow/dags/steam/data/extract/user/"
    df = spark.read.option("multiline", "true").json(path)

    df = df.withColumn("file_name",input_file_name())

    df = df.withColumn("file_date", regexp_extract(col("file_name"), r'\d{4}-\d{2}-\d{2}', 0))
    df = df.withColumn("file_date", to_date(col("file_date"), "yyyy-MM-dd"))

    window_spec = Window.partitionBy("appid").orderBy(col("file_date").desc())

    df = df.withColumn("row_number", row_number().over(window_spec))

    df = df.filter(col("row_number") == 1).drop("row_number")

    df = df.withColumn("package_groups", to_json(col("package_groups")))

    df = df.withColumn("pc_requirements_minimum", col("pc_requirements.minimum"))
    df = df.withColumn("pc_requirements_recommended", col("pc_requirements.recommended"))

    df = df.drop("pc_requirements")

    df.show()

    output_path = "/home/fececa/airflow/dags/steam/data/bronze/user"
    os.makedirs(output_path, exist_ok=True)
    df.write.format('avro') \
        .mode('overwrite') \
        .partitionBy('file_date') \
        .option('overwriteSchema', 'true') \
        .save(output_path)

    print(f"DataFrame salvo no formato Avro em: {output_path}")
