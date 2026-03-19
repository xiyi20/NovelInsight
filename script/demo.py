import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import desc
from pyspark.sql.window import Window

# 环境配置
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['HADOOP_USER_NAME'] = 'hadoop'

# JDBC 配置常量
JDBC_URL = "jdbc:mysql://node1:3306/bigdata?useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=True"
JDBC_PROPS = {
    "user": "root",
    "password": "123456",
    "driver": "com.mysql.cj.jdbc.Driver"
}


def reclassify_channels(df):
    """
    函数一：根据互动率、阅读量排名以及题材，重新定义男频/女频
    """
    # 1. 计算原始互动比例
    df_base = df.withColumn("raw_ratio", (F.col("monthTicker") + 1) / (F.col("allFlower") + 1))

    # 2. 定义窗口：按类型分区
    window_type = Window.partitionBy("type")

    # 3. 计算排名百分比
    # ratio_rank: 互动倾向排名（越小越像女频）；read_rank: 阅读量排名（越小越火）
    df_ranked = df_base \
        .withColumn("ratio_rank", F.percent_rank().over(window_type.orderBy("raw_ratio"))) \
        .withColumn("read_rank", F.percent_rank().over(window_type.orderBy(F.col("allRead").desc())))

    # 4. 执行“流量保护”分频逻辑
    df_final = df_ranked.withColumn("channel",
                                    F.when(F.col("type").isin(['玄幻奇幻', '军事历史', '科幻网游']),
                                           F.when((F.col("read_rank") > 0.15) & (F.col("ratio_rank") < 0.40),
                                                  "女频").otherwise("男频")
                                           ).when(F.col("type").isin(['青春校园', '同人小说']),
                                                  F.when((F.col("read_rank") > 0.15) & (F.col("ratio_rank") > 0.8),
                                                         "男频").otherwise("女频")
                                                  ).when(F.col("type") == "都市言情",
                                                         F.when(F.col("ratio_rank") > 0.7, "男频").otherwise("女频")
                                                         ).otherwise("其他")
                                    )

    # 清理中间计算列
    exclude_cols = ["raw_ratio", "ratio_rank", "read_rank"]
    return df_final.select([c for c in df_final.columns if c not in exclude_cols])


def calculate_hot_scores(df):
    """
    函数二：在各频道内部进行归一化处理，计算最终热度得分 (0-10分)
    """
    # 1. 定义原始互动得分
    df_interact = df.withColumn("interact_raw",
                                F.col("allFlower") * 0.5 + F.col("monthTicker") * 0.3 + F.col("shareNum") * 0.2
                                )

    # 2. 分频道归一化窗口
    window_channel = Window.partitionBy("channel")

    # 3. 计算频道内对数最大值并加权求和
    final_df = df_interact \
        .withColumn("max_read_log", F.max(F.log10(F.col("allRead") + 1)).over(window_channel)) \
        .withColumn("max_word_log", F.max(F.log10(F.col("wordNum") + 1)).over(window_channel)) \
        .withColumn("max_inter_log", F.max(F.log10(F.col("interact_raw") + 1)).over(window_channel)) \
        .withColumn("hotScore", F.round(
        (
                (F.log10(F.col("allRead") + 1) / F.col("max_read_log") * 0.3) +
                (F.log10(F.col("wordNum") + 1) / F.col("max_word_log") * 0.2) +
                (F.log10(F.col("interact_raw") + 1) / F.col("max_inter_log") * 0.4) +
                (F.col("authorDays") / 3000 * 0.1)
        ) * 10, 2)
                    )

    # 4. 筛选最终展示列
    return final_df.select(
        "id", "title", "type", "channel", "wordNum", "allRead", "allFlower", "reward", "hotScore"
    ).orderBy(F.col("hotScore").desc())


if __name__ == '__main__':
    # 初始化 Spark
    spark = SparkSession.builder.appName("NovelInsightPro").master("local[*]"). \
        config("spark.sql.shuffle.partitions", 2). \
        config("spark.jars", r"D:\Code\NovelInsight\script\mysql-connector-j-8.0.33.jar"). \
        config("spark.sql.warehouse.dir", "hdfs://node1:8020/user/hive/warehouse"). \
        config("hive.metastore.uris", "thrift://node1:9083"). \
        enableHiveSupport(). \
        getOrCreate()

    # --- 数据读取 ---
    mysql_df = spark.read.format("jdbc").option("url", JDBC_URL).option("dbtable", "novelData").options(
        **JDBC_PROPS).load()
    mysql_df.write.mode("overwrite").saveAsTable("novelData", "parquet")
    # -------------------------------
    # author_window = Window.partitionBy("author").orderBy(F.col("allRead").desc())
    #
    # # 提取每个作者阅读量第一的那部作品所在的频道
    # df_author_channel = mysql_df.withColumn("rn", F.row_number().over(author_window)) \
    #     .filter(F.col("rn") == 1) \
    #     .select("author", F.col("channel").alias("channel"))
    #
    # author_base = mysql_df.groupBy("author").agg(
    #     F.count("id").alias("workCount"),  # 作品数量
    #     F.sum("allRead").alias("totalRead"),  # 总阅读量（替代收藏量）
    #     F.sum("allFlower").alias("totalFlower"),  # 总鲜花（粉丝活跃度）
    #     F.max("authorWords").alias("authorWords"),  # 作者总字数
    #     F.max("authorDays").alias("authorDays"),  # 创作天数
    # )
    # author_base = author_base.join(df_author_channel, "author", "left")
    # # --- 2. 构建影响力模型 (加权得分) ---
    # # 定义权重：阅读(40%) + 活跃(20%) + 产出(20%) + 稳定性(20%)
    # # 使用 log 平滑处理极端值，再归一化到 0-100
    # author_model = author_base.withColumn(
    #     "score",
    #     F.round(
    #         (F.log10(F.col("totalRead") + 1) * 4.0) +
    #         (F.log10(F.col("totalFlower") + 1) * 2.0) +
    #         (F.col("workCount") * 1.5) +
    #         (F.log10(F.col("authorDays") + 1) * 2.5),
    #         2)
    # )
    #
    # # --- 3. 作者画像分类 (识别头部与新锐) ---
    # # 使用百分位排名定义等级
    # author_ranked = author_model.withColumn("rank_pct", F.percent_rank().over(Window.orderBy(F.col("score").desc())))
    #
    # author_profile = author_ranked.withColumn("level",
    #                                           F.when(F.col("rank_pct") < 0.05, "白金大神")
    #                                           .when(F.col("rank_pct") < 0.20, "资深名家")
    #                                           .when((F.col("rank_pct") < 3.0) & (F.col("authorDays") < 100),
    #                                                 "潜力新锐")
    #                                           .otherwise("普通作者")
    #                                           )
    # author_profile = author_profile.select([x for x in author_profile.columns if x not in ["rank_pct","score"]]).withColumn("id", monotonically_increasing_id())
    # author_profile.write.mode("overwrite").format("jdbc") \
    #     .option("url", JDBC_URL).option("dbtable", "authorProfile").options(**JDBC_PROPS).save()
    # author_profile.write.mode("overwrite").saveAsTable("authorProfile","parquet")
    # ---------------------------------

    # # --- 逻辑处理 ---
    # # 1. 重新划分频道
    # classified_df = reclassify_channels(raw_mysql_df)
    #
    # # 将处理后的频道结果写回 MySQL (更新 novelData)
    # classified_df.orderBy("id").write.mode("overwrite").format("jdbc") \
    #     .option("url", JDBC_URL).option("dbtable", "novelData").options(**JDBC_PROPS).save()
    #
    # # 2. 计算热度得分 (基于上一步处理完的 classified_df)
    # hot_scored_df = calculate_hot_scores(classified_df)
    #
    # # --- 数据输出 ---
    # # 写入 MySQL novelHot 表
    # hot_scored_df.write.mode("overwrite").format("jdbc") \
    #     .option("url", JDBC_URL).option("dbtable", "novelHot").options(**JDBC_PROPS).save()
    #
    # # 写入 Hive 表
    # hot_scored_df.write.mode("overwrite").saveAsTable("novelHot", "parquet")
    #
    # print("集群计算任务已完成！")
    # spark.stop()