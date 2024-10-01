from pyspark.sql import SparkSession

def gold_wish_steam():

    spark = SparkSession.builder \
        .appName("GoldStep") \
        .config("spark.jars", "/usr/share/java/mysql-connector-java-9.0.0.jar") \
        .getOrCreate()


    caminho_pasta = "/home/fececa/airflow/dags/steam/data/silver/wishlist"

    df = spark.read.format("parquet").load(caminho_pasta)

    df.printSchema()

    df.show()

    url = "jdbc:mysql://localhost:3306/silver"
    properties = {
        "user": "root",
        "password": "Fececa13",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    df.write \
    .jdbc(url=url, table="wishlist_steam", mode="overwrite", properties=properties)