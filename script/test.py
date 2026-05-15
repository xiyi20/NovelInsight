import os

import pyspark.sql.functions as F
# 导包
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, max, month, year

os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['HADOOP_USER_NAME'] = 'hadoop'
if __name__ == '__main__':
    # 构建
    spark = SparkSession.builder.appName("sparkSQL").master("local[*]"). \
        config("spark.sql.shuffle.partitions", 2). \
        config("spark.jars", r"D:\Code\NovelInsight\script\mysql-connector-j-8.0.33.jar"). \
        config("spark.sql.warehouse.dir.", "hdfs://node1:8020/user/hive/warehouse"). \
        config("hive.metastore.uris", "thrift://node1:9083"). \
        enableHiveSupport(). \
        getOrCreate()

    # 读取
    novelData = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://node1:3306/bigdata") \
        .option("dbtable", "novelData") \
        .option("user", "root") \
        .option("password", "123456") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .load()


    spark.sql("select * from TopRead").show()

    spark.sql("select * from TopReward").show()


    spark.sql("select * from typeAvgRead").show()