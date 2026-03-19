# coding:utf8
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
        config("spark.jars", r"D:\Code\NovelInsight\spark\mysql-connector-j-8.0.33.jar"). \
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

    ''' 
        需求1 
    '''
    result1 = novelData.orderBy("allRead", ascending=False).limit(10)
    # sql
    # result1: TopRead表
    result1.write.mode("overwrite"). \
        format("jdbc"). \
        option("url", "jdbc:mysql://node1:3306/bigdata?useSSL=false&useUnicode=true&charset=utf8"). \
        option("dbtable", "TopRead"). \
        option("user", "root"). \
        option("password", "123456"). \
        option("encoding", "utf-8"). \
        option("driver", "com.mysql.cj.jdbc.Driver"). \
        save()
    result1.write.mode("overwrite").saveAsTable("TopRead", "parquet")
    spark.sql("select * from TopRead").show()

    ''' 
        需求2
    '''
    result2 = novelData.orderBy("reward", ascending=False).limit(10)
    # sql
    # result2: TopReward表
    result2.write.mode("overwrite"). \
        format("jdbc"). \
        option("url", "jdbc:mysql://node1:3306/bigdata?useSSL=false&useUnicode=true&charset=utf8"). \
        option("dbtable", "TopReward"). \
        option("user", "root"). \
        option("password", "123456"). \
        option("encoding", "utf-8"). \
        option("driver", "com.mysql.cj.jdbc.Driver"). \
        save()
    result2.write.mode("overwrite").saveAsTable("TopReward", "parquet")
    spark.sql("select * from TopReward").show()

    ''' 
        需求3
    '''
    result3 = novelData.groupby("type").agg(avg("allRead").alias("avg_allRead"))
    # sql
    # result3: typeAvgRead表
    result3.write.mode("overwrite"). \
        format("jdbc"). \
        option("url", "jdbc:mysql://node1:3306/bigdata?useSSL=false&useUnicode=true&charset=utf8"). \
        option("dbtable", "typeAvgRead"). \
        option("user", "root"). \
        option("password", "123456"). \
        option("encoding", "utf-8"). \
        option("driver", "com.mysql.cj.jdbc.Driver"). \
        save()
    result3.write.mode("overwrite").saveAsTable("typeAvgRead", "parquet")
    spark.sql("select * from typeAvgRead").show()

    ''' 
        需求4
    '''
    result4 = novelData.select("monthTicker", "title").orderBy("monthTicker", ascending=False).limit(10)
    # sql
    # result4: TopMTicket表
    result4.write.mode("overwrite"). \
        format("jdbc"). \
        option("url", "jdbc:mysql://node1:3306/bigdata?useSSL=false&useUnicode=true&charset=utf8"). \
        option("dbtable", "TopMTicket"). \
        option("user", "root"). \
        option("password", "123456"). \
        option("encoding", "utf-8"). \
        option("driver", "com.mysql.cj.jdbc.Driver"). \
        save()
    result4.write.mode("overwrite").saveAsTable("TopMTicket", "parquet")
    spark.sql("select * from TopMTicket").show()

    ''' 
        需求5
    '''
    result5 = novelData.groupby("type").agg(
        max("allRead").alias("max_allRead"),
        max("allFlower").alias("max_allFlower")
    )
    # sql
    # result5: novelTypeMaxRF表
    result5.write.mode("overwrite"). \
        format("jdbc"). \
        option("url", "jdbc:mysql://node1:3306/bigdata?useSSL=false&useUnicode=true&charset=utf8"). \
        option("dbtable", "novelTypeMaxRF"). \
        option("user", "root"). \
        option("password", "123456"). \
        option("encoding", "utf-8"). \
        option("driver", "com.mysql.cj.jdbc.Driver"). \
        save()
    result5.write.mode("overwrite").saveAsTable("novelTypeMaxRF", "parquet")
    spark.sql("select * from novelTypeMaxRF").show()

    ''' 
        需求6 类型推荐
    '''
    max_share_df = novelData.groupby("type").agg(F.max("shareNum").alias("max_share"))

    novelData_alias = novelData.alias("novel")
    max_share_df_alias = max_share_df.alias("maxShare")

    result6 = novelData_alias.join(max_share_df,
                                   (novelData_alias.type == max_share_df.type) &
                                   (novelData_alias.shareNum == max_share_df.max_share),
                                   "inner"
                                   ) \
        .select(
        novelData_alias.type.alias("type"),
        novelData_alias.title.alias("title"),
        max_share_df.max_share.alias("maxShare")
    )
    # sql
    # result6: novelMaxShare表
    result6.write.mode("overwrite"). \
        format("jdbc"). \
        option("url", "jdbc:mysql://node1:3306/bigdata?useSSL=false&useUnicode=true&charset=utf8"). \
        option("dbtable", "novelMaxShare"). \
        option("user", "root"). \
        option("password", "123456"). \
        option("encoding", "utf-8"). \
        option("driver", "com.mysql.cj.jdbc.Driver"). \
        save()
    result6.write.mode("overwrite").saveAsTable("novelMaxShare", "parquet")
    spark.sql("select * from novelMaxShare").show()

    '''
        需求7
    '''
    result7 = novelData.groupby("type").count()
    # sql
    # result7: novelType表
    result7.write.mode("overwrite"). \
        format("jdbc"). \
        option("url", "jdbc:mysql://node1:3306/bigdata?useSSL=false&useUnicode=true&charset=utf8"). \
        option("dbtable", "novelType"). \
        option("user", "root"). \
        option("password", "123456"). \
        option("encoding", "utf-8"). \
        option("driver", "com.mysql.cj.jdbc.Driver"). \
        save()
    result7.write.mode("overwrite").saveAsTable("novelType", "parquet")
    spark.sql("select * from novelType").show()

    '''
        需求8
    '''
    novelData_with_range = novelData.withColumn(
        "read_range",
        F.when(novelData.allRead < 500000, "0-500000")
        .when((novelData.allRead >= 500000) & (novelData.allRead < 1000000), "500000-1000000")
        .when((novelData.allRead >= 1000000) & (novelData.allRead < 2000000), "1000000-2000000")
        .when((novelData.allRead >= 2000000) & (novelData.allRead < 5000000), "2000000-5000000")
        .when((novelData.allRead >= 5000000) & (novelData.allRead < 10000000), "5000000-10000000")
        .otherwise("10000000以上")
    )
    result8 = novelData_with_range.groupby("type", "read_range").count().orderBy("type", "read_range")
    #
    result8.write.mode("overwrite"). \
        format("jdbc"). \
        option("url", "jdbc:mysql://node1:3306/bigdata?useSSL=false&useUnicode=true&charset=utf8"). \
        option("dbtable", "novelReadRange"). \
        option("user", "root"). \
        option("password", "123456"). \
        option("encoding", "utf-8"). \
        option("driver", "com.mysql.cj.jdbc.Driver"). \
        save()
    result8.write.mode("overwrite").saveAsTable("novelReadRange", "parquet")
    spark.sql("select * from novelReadRange").show()

    '''
        需求9
    '''
    novelData_with_range2 = novelData.withColumn(
        "wordNum_range",
        F.when(novelData.wordNum < 50000, "0-50000")
        .when((novelData.wordNum >= 50000) & (novelData.wordNum < 100000), "50000-100000")
        .when((novelData.wordNum >= 100000) & (novelData.wordNum < 200000), "100000-200000")
        .when((novelData.wordNum >= 200000) & (novelData.wordNum < 500000), "200000-500000")
        .when((novelData.wordNum >= 500000) & (novelData.wordNum < 1000000), "500000-1000000")
        .otherwise("1000000以上")
    )
    result9 = novelData_with_range2.groupby("type", "wordNum_range").count().orderBy("type", "wordNum_range")
    #
    result9.write.mode("overwrite"). \
        format("jdbc"). \
        option("url", "jdbc:mysql://node1:3306/bigdata?useSSL=false&useUnicode=true&charset=utf8"). \
        option("dbtable", "novelWordNumRange"). \
        option("user", "root"). \
        option("password", "123456"). \
        option("encoding", "utf-8"). \
        option("driver", "com.mysql.cj.jdbc.Driver"). \
        save()
    result9.write.mode("overwrite").saveAsTable("novelWordNumRange", "parquet")
    spark.sql("select * from novelWordNumRange").show()

    '''
        需求10
    '''
    result10 = novelData.groupby("type").agg(F.max("allFlower").alias("max_allFlower"))
    result10 = result10.orderBy(F.desc("max_allFlower"))
    #
    # result10.write.mode("overwrite").saveAsTable("novelReadRange", "parquet")
    # spark.sql("select * from novelReadRange").show()
    #
    result10.write.mode("overwrite"). \
        format("jdbc"). \
        option("url", "jdbc:mysql://node1:3306/bigdata?useSSL=false&useUnicode=true&charset=utf8"). \
        option("dbtable", "novelFlowerTop"). \
        option("user", "root"). \
        option("password", "123456"). \
        option("encoding", "utf-8"). \
        option("driver", "com.mysql.cj.jdbc.Driver"). \
        save()
    result10.write.mode("overwrite").saveAsTable("novelFlowerTop", "parquet")
    spark.sql("select * from novelFlowerTop").show()

    '''
        需求11
    '''
    result11 = novelData.select("author", "authorDays") \
        .orderBy("authorDays", ascending=False) \
        .limit(10)
    #
    result11.write.mode("overwrite"). \
        format("jdbc"). \
        option("url", "jdbc:mysql://node1:3306/bigdata?useSSL=false&useUnicode=true&charset=utf8"). \
        option("dbtable", "authorDayTop"). \
        option("user", "root"). \
        option("password", "123456"). \
        option("encoding", "utf-8"). \
        option("driver", "com.mysql.cj.jdbc.Driver"). \
        save()
    result11.write.mode("overwrite").saveAsTable("authorDayTop", "parquet")
    spark.sql("select * from authorDayTop").show()

    '''
        需求12
    '''
    author_global_stats = novelData.groupBy("author").agg(
        F.max("authorWords").alias("wordNum"),
    )

    result12 = author_global_stats.orderBy(F.col("wordNum").desc()).limit(10)
    #
    result12.write.mode("overwrite"). \
        format("jdbc"). \
        option("url", "jdbc:mysql://node1:3306/bigdata?useSSL=false&useUnicode=true&charset=utf8"). \
        option("dbtable", "authorMaxWord"). \
        option("user", "root"). \
        option("password", "123456"). \
        option("encoding", "utf-8"). \
        option("driver", "com.mysql.cj.jdbc.Driver"). \
        save()
    result12.write.mode("overwrite").saveAsTable("authorMaxWord", "parquet")
    spark.sql("select * from authorMaxWord").show()

    '''
        需求13
    '''
    monthly_data = novelData.withColumn("month", month(novelData["startTime"]))
    result13 = monthly_data.groupby("month").agg(
        count('*').alias("data_count"),
        avg("rate").alias("avg_rate")
    )
    #
    result13.write.mode("overwrite"). \
        format("jdbc"). \
        option("url", "jdbc:mysql://node1:3306/bigdata?useSSL=false&useUnicode=true&charset=utf8"). \
        option("dbtable", "novelMonthCount"). \
        option("user", "root"). \
        option("password", "123456"). \
        option("encoding", "utf-8"). \
        option("driver", "com.mysql.cj.jdbc.Driver"). \
        save()
    result13.write.mode("overwrite").saveAsTable("novelMonthCount", "parquet")
    spark.sql("select * from novelMonthCount").show()

    '''
        需求14
    '''
    result14 = monthly_data.groupby("month").agg(
        max("monthRead").alias("max_monthRead")
    )
    #
    result14.write.mode("overwrite"). \
        format("jdbc"). \
        option("url", "jdbc:mysql://node1:3306/bigdata?useSSL=false&useUnicode=true&charset=utf8"). \
        option("dbtable", "novelMonthRead"). \
        option("user", "root"). \
        option("password", "123456"). \
        option("encoding", "utf-8"). \
        option("driver", "com.mysql.cj.jdbc.Driver"). \
        save()
    result14.write.mode("overwrite").saveAsTable("novelMonthRead", "parquet")
    spark.sql("select * from novelMonthRead").show()

    '''
        需求15
    '''
    yearly_data = novelData.withColumn("year", year(novelData["startTime"]))
    result15 = yearly_data.groupby("year").agg(
        max("allRead").alias("max_allRead")
    )
    #
    result15.write.mode("overwrite"). \
        format("jdbc"). \
        option("url", "jdbc:mysql://node1:3306/bigdata?useSSL=false&useUnicode=true&charset=utf8"). \
        option("dbtable", "novelYearRead"). \
        option("user", "root"). \
        option("password", "123456"). \
        option("encoding", "utf-8"). \
        option("driver", "com.mysql.cj.jdbc.Driver"). \
        save()
    result15.write.mode("overwrite").saveAsTable("novelYearRead", "parquet")
    spark.sql("select * from novelYearRead").show()
