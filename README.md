# NovelInsight

小说洞察系统 - 基于 Spring Boot 3 和大数据技术栈的网络小说数据分析平台

## 项目简介

NovelInsight 是一个综合性的网络小说数据分析与可视化平台，集成了数据采集、大数据分析、数据可视化和智能推荐等功能。系统通过爬虫技术采集网络小说数据，利用 Spark 进行大数据分析，通过 Web 界面展示多维度的数据分析结果。

## 技术栈

### 后端技术
- **Spring Boot 3.2.0** - 核心框架
- **Java 17** - 开发语言
- **Spring Security** - 安全认证与授权
- **Spring Data JPA** - 数据持久化
- **Thymeleaf** - 模板引擎

### 数据库与大数据
- **MySQL 8.0** - 关系型数据库
- **Apache Hive 3.1.3** - 数据仓库
- **Apache Spark** - 大数据处理引擎
- **PySpark** - Spark Python API

### 前端技术
- **Bootstrap** - UI 框架
- **ECharts** - 数据可视化
- **Chart.js** - 图表库
- **jQuery** - JavaScript 库

### 工具库
- **Lombok** - 简化 Java 代码
- **HikariCP** - 数据库连接池
- **lxml** - Python XML/HTML 解析
- **pandas** - Python 数据分析

## 项目结构

```
NovelInsight/
├── script/                      # Python 脚本目录
│   ├── spiderIndex.py          # 索引页爬虫
│   ├── spiderDetail.py         # 详情页爬虫
│   ├── demo.py                 # Spark 数据导入
│   ├── sparkAna.py             # Spark 数据分析
│   ├── sparkFir.py             # Spark 首次数据加载
│   ├── repairAuthor.py         # 作者数据修复
│   └── *.csv                   # 数据文件
├── src/main/java/com/novelinsight/
│   ├── config/                 # 配置类
│   │   ├── DataSourceConfig.java
│   │   ├── JpaConfig.java
│   │   ├── SecurityConfig.java
│   │   └── ThymeleafConfig.java
│   ├── controller/             # 控制器
│   │   ├── AuthController.java
│   │   ├── ChartController.java
│   │   ├── DataController.java
│   │   ├── HomeController.java
│   │   ├── RecommendController.java
│   │   └── UserController.java
│   ├── model/                  # 数据模型
│   │   ├── User.java
│   │   └── History.java
│   ├── repository/             # 数据访问层
│   │   ├── UserRepository.java
│   │   └── HistoryRepository.java
│   ├── service/                # 业务逻辑层
│   │   ├── security/
│   │   │   ├── CustomPasswordEncoder.java
│   │   │   └── CustomUserDetailsService.java
│   │   ├── ChartDataService.java
│   │   ├── HistoryService.java
│   │   ├── HiveService.java
│   │   ├── MySqlQueryService.java
│   │   ├── PublicDataService.java
│   │   ├── RecommendationService.java
│   │   └── UserService.java
│   └── NovelInsightApplication.java
├── src/main/resources/
│   ├── static/                 # 静态资源
│   │   └── assets/
│   │       ├── css/
│   │       ├── js/
│   │       ├── img/
│   │       └── fonts/
│   ├── templates/              # Thymeleaf 模板
│   │   ├── index.html
│   │   ├── login.html
│   │   ├── register.html
│   │   ├── recommend.html
│   │   ├── novelCloud.html
│   │   ├── authorChart.html
│   │   ├── typeChart.html
│   │   ├── timeChart.html
│   │   ├── infoChart.html
│   │   ├── tableData.html
│   │   └── collectData.html
│   └── application.yml         # 应用配置
├── pom.xml                     # Maven 配置
└── README.md                   # 项目说明文档
```

## 主要功能

### 1. 数据采集
- **索引页爬虫** (spiderIndex.py): 批量采集小说列表页链接
- **详情页爬虫** (spiderDetail.py): 采集小说详细信息，包括：
  - 小说类型、标题、封面
  - 作者信息（头像、作品数、总字数、创作天数）
  - 阅读数据（月阅读、月鲜花、总阅读、总鲜花）
  - 互动数据（更新票、打赏、月票、分享数）
  - 评分、开始时间、更新时间

### 2. 大数据分析
使用 PySpark 进行多维度数据分析，生成 15+ 个分析报表：
- **TopRead**: 总阅读量 Top10 小说
- **TopReward**: 打赏金额 Top10 小说
- **typeAvgRead**: 各类型平均阅读量
- **TopMTicket**: 月票 Top10 小说
- **novelTypeMaxRF**: 各类型最大阅读量和鲜花数
- **novelMaxShare**: 各类型分享数最高的小说
- **novelType**: 各类型小说数量统计
- **novelReadRange**: 各类型阅读量区间分布
- **novelWordNumRange**: 各类型字数区间分布
- **novelFlowerTop**: 各类型鲜花数排行
- **authorDayTop**: 创作天数 Top10 作者
- **authorMaxWord**: 各类型字数最多的作者
- **novelMonthCount**: 按月统计小说数量和平均评分
- **novelMonthRead**: 按月统计最大阅读量
- **novelYearRead**: 按年统计最大阅读量

### 3. 数据可视化
- **首页仪表盘**: 展示关键指标和趋势图
- **作者分析图表**: 作者相关数据可视化
- **类型分析图表**: 小说类型分布和对比
- **时间分析图表**: 时间维度数据分析
- **信息分析图表**: 多维度信息展示
- **词云展示**: 小说关键词云图
- **表格数据**: 详细数据表格展示

### 4. 用户系统
- 用户注册与登录
- 基于 Spring Security 的安全认证
- 用户浏览历史记录
- 个性化推荐

### 5. 推荐系统
- 基于协同过滤的推荐算法
- 类型推荐
- 热门推荐

## 环境要求

- **JDK**: 17+
- **Maven**: 3.6+
- **MySQL**: 8.0+
- **Hadoop**: 3.x
- **Hive**: 3.1.3+
- **Spark**: 3.x
- **Python**: 3.8+ (用于爬虫和 Spark 脚本)

## 快速开始

### 1. 克隆项目

```bash
git clone <repository-url>
cd NovelInsight
```

### 2. 配置数据库

修改 `src/main/resources/application.yml` 中的数据库配置：

```yaml
spring:
  datasource:
    novel:
      url: jdbc:mysql://127.0.0.1:3306/novelData
      username: root
      password: your_password
    bigdata:
      url: jdbc:mysql://node1:3306/bigdata
      username: root
      password: your_password
    hive:
      url: jdbc:hive2://node1:10000/default
      username: hadoop
```

### 3. 构建项目

```bash
mvn clean package
```

### 4. 运行应用

```bash
mvn spring-boot:run
```

或直接运行 JAR 包：

```bash
java -jar target/NovelInsight-1.0.0.jar
```

### 5. 访问应用

打开浏览器访问：`http://localhost:8080`

## 数据采集流程

### 1. 运行索引页爬虫

```bash
cd script
python spiderIndex.py
```

### 2. 运行详情页爬虫

```bash
python spiderDetail.py
```

### 3. 导入数据到 Hive

```bash
python sparkFir.py
```

### 4. 运行数据分析

```bash
python sparkAna.py
```

## 数据库表结构

### 用户相关表
- **user**: 用户信息表
- **history**: 用户浏览历史表

### 小说数据表
- **novelData**: 原始小说数据表
- **TopRead**: 阅读量 Top10
- **TopReward**: 打赏 Top10
- **typeAvgRead**: 类型平均阅读量
- **TopMTicket**: 月票 Top10
- **novelTypeMaxRF**: 类型最大阅读鲜花
- **novelMaxShare**: 类型最大分享
- **novelType**: 类型统计
- **novelReadRange**: 阅读量区间分布
- **novelWordNumRange**: 字数区间分布
- **novelFlowerTop**: 鲜花排行
- **authorDayTop**: 作者创作天数排行
- **authorMaxWord**: 作者字数排行
- **novelMonthCount**: 月度统计
- **novelMonthRead**: 月度阅读
- **novelYearRead**: 年度阅读

## 配置说明

### application.yml 主要配置项

```yaml
# 数据源配置
spring.datasource.novel.url: 本地 MySQL 连接
spring.datasource.bigdata.url: 集群 MySQL 连接
spring.datasource.hive.url: Hive 连接

# JPA 配置
spring.jpa.hibernate.ddl-auto: update  # 自动更新表结构

# 会话超时
spring.session.timeout: 1800  # 30分钟

# 自定义配置
app.hive.query-timeout: 120000  # Hive 查询超时
app.recommendation.cache-duration: 3600  # 推荐缓存时间
app.recommendation.top-n: 3  # 推荐数量
```

## 开发指南

### 添加新的数据分析

1. 在 `script/` 目录下创建新的 Python 脚本
2. 使用 PySpark API 进行数据处理
3. 将结果写入 MySQL 或 Hive 表
4. 在 Java Service 层添加查询方法
5. 在 Controller 层添加接口
6. 在前端添加可视化展示

### 添加新的页面

1. 在 `src/main/resources/templates/` 创建 HTML 模板
2. 在 Controller 添加对应的映射方法
3. 在 Service 层添加业务逻辑
4. 在 `sidebar.html` 添加导航链接

## 常见问题

### 1. Hive 连接失败
- 检查 Hive 服务是否启动
- 确认 Hadoop 环境变量配置正确
- 检查防火墙设置

### 2. Spark 任务运行缓慢
- 调整 `spark.sql.shuffle.partitions` 参数
- 增加 executor 内存
- 优化 SQL 查询

### 3. 爬虫被反爬
- 增加 User-Agent 轮换
- 添加代理 IP
- 增加请求间隔时间

## 许可证

本项目仅供学习和研究使用。

## 联系方式

如有问题或建议，请联系项目维护者。

---

**注意**: 本项目涉及的数据采集请遵守相关网站的 robots.txt 协议和法律法规。