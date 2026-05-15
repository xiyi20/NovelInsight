# coding:utf8
import os

# 导包
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType

os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['HADOOP_USER_NAME'] = 'hadoop'
if __name__ == '__main__':
    # 构建
    spark = SparkSession.builder.appName("sparkSQL").master("local[*]"). \
        config("spark.sql.shuffle.partitions", 2). \
        config("spark.jars", r"D:\Code\NovelInsight\spark\mysql-connector-j-8.0.33.jar"). \
        config("spark.sql.warehouse.dir", "hdfs://node1:8020/user/hive/warehouse"). \
        config("hive.metastore.uris", "thrift://node1:9083"). \
        enableHiveSupport(). \
        getOrCreate()

    schema = StructType()
    schema = schema.add("type", StringType(), nullable=True). \
        add("title", StringType(), nullable=True). \
        add("cover", StringType(), nullable=True). \
        add("author", StringType(), nullable=True). \
        add("authorImg", StringType(), nullable=True). \
        add("authorWork", IntegerType(), nullable=True). \
        add("authorWords", IntegerType(), nullable=True). \
        add("authorDays", IntegerType(), nullable=True). \
        add("monthRead", IntegerType(), nullable=True). \
        add("monthFlower", IntegerType(), nullable=True). \
        add("allRead", IntegerType(), nullable=True). \
        add("allFlower", IntegerType(), nullable=True). \
        add("wordNum", IntegerType(), nullable=True). \
        add("updateTicket", IntegerType(), nullable=True). \
        add("reward", FloatType(), nullable=True). \
        add("monthTicker", IntegerType(), nullable=True). \
        add("shareNum", IntegerType(), nullable=True). \
        add("rate", FloatType(), nullable=True). \
        add("startTime", StringType(), nullable=True). \
        add("updateTime", StringType(), nullable=True). \
        add("detailLink", StringType(), nullable=True)

    # 读取数据
    df = spark.read.format("csv"). \
        option("sep", ","). \
        option("header", True). \
        option("encoding", "utf-8"). \
        schema(schema=schema). \
        load("./novelData.csv")  # 读取novelData

    # 数据清洗
    df.drop_duplicates()
    df.na.drop()  # 去掉空值

    # 给表加一个自增id列
    df = df.withColumn("id", monotonically_increasing_id())

    # sql
    df.write.mode("overwrite"). \
        format("jdbc"). \
        option("url", "jdbc:mysql://node1:3306/bigdata?useSSL=false&useUnicode=true&charset=utf8"). \
        option("dbtable", "novelData"). \
        option("user", "root"). \
        option("password", "123456"). \
        option("encoding", "utf-8"). \
        option("driver", "com.mysql.cj.jdbc.Driver"). \
        save()

    df.write.mode("overwrite").saveAsTable("novelData", "parquet")
    spark.sql("select * from novelData").show()
