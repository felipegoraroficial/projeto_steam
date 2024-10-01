from pyspark.sql.functions import col,when,udf,round,expr,to_date,regexp_replace
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType, LongType
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
import os

def silver_wish_steam():

    schema = StructType([
        StructField("appid", LongType(), True),
        StructField("file_data", DateType(), True),
        StructField("img", StringType(), True),
        StructField("name", StringType(), True),
        StructField("price", FloatType(), True),
        StructField("short_description", StringType(), True),
        StructField("discount", LongType(), True),
        StructField("recommended", StringType(), True),
        StructField("minimum", StringType(), True),
        StructField("link", StringType(), True)
    ])


    spark = SparkSession.builder \
        .appName("SilverStep") \
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.0") \
        .getOrCreate()

    path = '/home/fececa/airflow/dags/steam/data/bronze/wishlist/'
    df = spark.read.format("avro").load(path)


    df = df.select("steam_appid", "file_data", "header_image", "name", "price",
                "short_description","discount_pct","recommended",
                "minimum")


    df = df.withColumnRenamed("discount_pct", "discount")\
        .withColumnRenamed("header_image", "img")


    df = df.withColumn("price",regexp_replace(col("price"),"R\\$",""))
    df = df.withColumn("price",regexp_replace(col("price"),"\\.",""))
    df = df.withColumn("price",regexp_replace(col("price"),",","."))
    df = df.withColumn("price", round(col("price").cast("float"), 2))

    df = df.withColumn("steam_appid", col("steam_appid").cast("long"))


    def extrair_texto_html(html):
        if html is None:
            return ""
        soup = BeautifulSoup(html, 'html.parser')
        texto = soup.get_text()
        return texto

    extrair_texto_html_udf = udf(extrair_texto_html, StringType())

    df = df.withColumn('minimum', when((df['minimum'].isNull()) | (df['minimum'] == ""), "-").otherwise(extrair_texto_html_udf(df['minimum'])))
    df = df.withColumn('recommended', when((df['recommended'].isNull()) | (df['recommended'] == ""), "-").otherwise(extrair_texto_html_udf(df['recommended'])))


    df = df.withColumn('minimum', expr("regexp_replace(minimum, 'Minimum:', '')"))
    df = df.withColumn('minimum', expr("regexp_replace(minimum, 'Minimum', '')"))
    df = df.withColumn('recommended', expr("regexp_replace(recommended, 'Recommended:', '')"))
    df = df.withColumn('recommended', expr("regexp_replace(recommended, 'Recommended', '')"))


    df = df.withColumn("link", F.concat(F.lit("https://store.steampowered.com/app/"), F.col("steam_appid")))

    df = df.withColumn("file_data", to_date(df["file_data"], "yyyy-MM-dd"))

    df = spark.createDataFrame(df.rdd, schema)

    string_cols = [f.name for f in df.schema.fields if f.dataType == StringType()]

    for col_name in string_cols:
        df = df.withColumn(col_name, when(col(col_name).isNull(), '-').otherwise(col(col_name)))
        df = df.withColumn(col_name, when(col(col_name) == 'nan', '-').otherwise(col(col_name)))
        df = df.withColumn(col_name, when(col(col_name) == 'N/A', '-').otherwise(col(col_name)))

    df.printSchema()
    df.show()

    output_path = '/home/fececa/airflow/dags/steam/data/silver/wishlist/'
    os.makedirs(output_path, exist_ok=True)

    df.write.mode('overwrite').partitionBy('file_data').parquet(output_path)
