from pyspark.sql.functions import col,when,udf,round,expr,to_date
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType, LongType
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
import os

def silver_user_steam():

    schema = StructType([
        StructField("appid", LongType(), True),
        StructField("file_data", DateType(), True),
        StructField("img", StringType(), True),
        StructField("name", StringType(), True),
        StructField("playtime", FloatType(), True),
        StructField("short_description", StringType(), True),
        StructField("website", StringType(), True),
        StructField("recommended", StringType(), True),
        StructField("minimum", StringType(), True),
        StructField("link", StringType(), True)
    ])


    spark = SparkSession.builder \
        .appName("SilverStep") \
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.0") \
        .getOrCreate()

    path = '/home/fececa/airflow/dags/steam/data/bronze/user/'
    df = spark.read.format("avro").load(path)


    df = df.select("appid", "file_data", "header_image", "name", "playtime_forever",
                "short_description","website","pc_requirements_recommended",
                "pc_requirements_minimum")


    df = df.withColumnRenamed("playtime_forever", "playtime")\
        .withColumnRenamed("header_image", "img")



    df = df.withColumn("playtime", round((col("playtime").cast("float") / 60), 2))


    def extrair_texto_html(html):
        if html is None:
            return ""
        soup = BeautifulSoup(html, 'html.parser')
        texto = soup.get_text()
        return texto

    extrair_texto_html_udf = udf(extrair_texto_html, StringType())

    df = df.withColumn('pc_requirements_minimum', when((df['pc_requirements_minimum'].isNull()) | (df['pc_requirements_minimum'] == ""), "-").otherwise(extrair_texto_html_udf(df['pc_requirements_minimum'])))
    df = df.withColumn('pc_requirements_recommended', when((df['pc_requirements_recommended'].isNull()) | (df['pc_requirements_recommended'] == ""), "-").otherwise(extrair_texto_html_udf(df['pc_requirements_recommended'])))


    df = df.withColumn('pc_requirements_minimum', expr("regexp_replace(pc_requirements_minimum, 'pc_requirements_minimum:', '')"))
    df = df.withColumn('pc_requirements_minimum', expr("regexp_replace(pc_requirements_minimum, 'pc_requirements_minimum', '')"))
    df = df.withColumn('pc_requirements_recommended', expr("regexp_replace(pc_requirements_recommended, 'pc_requirements_recommended:', '')"))
    df = df.withColumn('pc_requirements_recommended', expr("regexp_replace(pc_requirements_recommended, 'pc_requirements_recommended', '')"))


    df = df.withColumn("link", F.concat(F.lit("https://store.steampowered.com/app/"), F.col("appid")))

    df = df.withColumn("file_data", to_date(df["file_data"], "yyyy-MM-dd"))

    df = spark.createDataFrame(df.rdd, schema)

    string_cols = [f.name for f in df.schema.fields if f.dataType == StringType()]

    for col_name in string_cols:
        df = df.withColumn(col_name, when(col(col_name).isNull(), '-').otherwise(col(col_name)))
        df = df.withColumn(col_name, when(col(col_name) == 'nan', '-').otherwise(col(col_name)))


    df.show()

    output_path = '/home/fececa/airflow/dags/steam/data/silver/user/'
    os.makedirs(output_path, exist_ok=True)

    df.write.mode('overwrite').partitionBy('file_data').parquet(output_path)
