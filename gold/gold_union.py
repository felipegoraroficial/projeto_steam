from pyspark.sql import SparkSession

def union_gold():

    spark = SparkSession.builder \
        .appName("MySQLReader") \
        .config("spark.jars", "/usr/share/java/mysql-connector-java-9.0.0.jar") \
        .getOrCreate()

    jdbcHostname = "localhost"
    jdbcPort = 3306
    jdbcDatabase = "silver"
    jdbcUsername = "root"
    jdbcPassword = "Fececa13"

    jdbcUrl = f"jdbc:mysql://{jdbcHostname}:{jdbcPort}/{jdbcDatabase}"

    connectionProperties = {
        "user": jdbcUsername,
        "password": jdbcPassword,
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    user = spark.read.jdbc(url=jdbcUrl, table="user_steam", properties=connectionProperties)
    wish = spark.read.jdbc(url=jdbcUrl, table="wishlist_steam", properties=connectionProperties)

    union = user.unionByName(wish,allowMissingColumns=True)

    union.printSchema()
    union.show()

    jdbcDatabaseGold = "gold"
    jdbcUrlGold = f"jdbc:mysql://{jdbcHostname}:{jdbcPort}/{jdbcDatabaseGold}"

    union.write \
    .jdbc(url=jdbcUrlGold, table="steam", mode="overwrite", properties=connectionProperties)