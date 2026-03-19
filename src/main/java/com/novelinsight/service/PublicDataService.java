package com.novelinsight.service;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 公共数据服务，对应Python中的getPublicData.py
 */
@Service
public class PublicDataService {

    private final HiveService hiveService;

    public PublicDataService(HiveService hiveService) {
        this.hiveService = hiveService;
    }

    /**
     * 获取所有小说数据
     */
    public List<Object[]> getNovelData() {
        String sql = "SELECT * FROM novelData";
        return hiveService.query(sql);
    }

    /**
     * 获取过滤和排序后的小说数据（带分页）
     *
     * @param typeFilter   类型过滤（可选）
     * @param authorFilter 作者过滤（可选）
     * @param novelFilter  小说名过滤（可选）
     * @param sortField    排序字段（可选，默认阅读量）
     * @param sortOrder    排序顺序（ASC/DESC，默认DESC）
     * @param page         页码（从0开始）
     * @param size         每页大小
     * @return 分页的小说数据
     */
    public Page<Object[]> getFilteredNovelData(String typeFilter, String authorFilter, String novelFilter,
                                               String sortField, String sortOrder, int page, int size) {
        // 1. 提取公共的 WHERE 子句构建逻辑
        StringBuilder whereClause = new StringBuilder(" FROM novelData WHERE 1=1");
        List<Object> params = new ArrayList<>();

        if (hasText(typeFilter)) {
            whereClause.append(" AND type LIKE ?");
            params.add("%" + typeFilter.trim() + "%");
        }
        if (hasText(authorFilter)) {
            whereClause.append(" AND author LIKE ?");
            params.add("%" + authorFilter.trim() + "%");
        }
        if (hasText(novelFilter)) {
            whereClause.append(" AND title LIKE ?");
            params.add("%" + novelFilter.trim() + "%");
        }

        // 2. 验证并构建排序子句
        String orderBy = " ORDER BY " + validateSortField(sortField) + " " + ("ASC".equalsIgnoreCase(sortOrder) ? "ASC" : "DESC");

        // 3. 异步并行执行：总记录数查询 和 数据分页查询
        CompletableFuture<Long> countFuture = CompletableFuture.supplyAsync(() -> {
            String countSql = "SELECT COUNT(*)" + whereClause;
            return hiveService.queryForObject(countSql, Long.class, params.toArray());
        });

        CompletableFuture<List<Object[]>> dataFuture = CompletableFuture.supplyAsync(() -> {
            // Spark SQL/Hive 分页方案：使用ROW_NUMBER()窗口函数
            long offset = (long) page * size;
            long endRow = offset + size;
            long startRow = offset + 1; // ROW_NUMBER从1开始

            // 构建基础查询（去除开头的" FROM"）
            String baseFromWhere = whereClause.toString();
            // 去掉开头的空格和"FROM"前的空格，得到"novelData WHERE 1=1..."
            String fromWherePart = baseFromWhere.trim();
            if (fromWherePart.startsWith("FROM ")) {
                fromWherePart = fromWherePart.substring(5);
            }

            String dataSql = "SELECT * FROM (" +
                    "SELECT *, ROW_NUMBER() OVER (" + orderBy + ") as rn " +
                    "FROM " + fromWherePart + ") t WHERE rn >= " + startRow + " AND rn <= " + endRow;

            return hiveService.query(dataSql, params.toArray());
        });

        // 4. 等待两者完成
        try {
            CompletableFuture.allOf(countFuture, dataFuture).join();
            return new PageImpl<>(dataFuture.get(), PageRequest.of(page, size), countFuture.get());
        } catch (Exception e) {
            throw new RuntimeException("集群查询超时或失败", e);
        }
    }

    // 辅助：检查字符串是否为空
    private boolean hasText(String str) {
        return str != null && !str.trim().isEmpty();
    }

    // 辅助：SQL注入防御及字段转换
    private String validateSortField(String sortField) {
        if (sortField == null) return "allRead";
        return switch (sortField.trim().toLowerCase()) {
            case "wordnum" -> "wordNum";
            case "allflower" -> "allFlower";
            case "reward" -> "reward";
            default -> "allRead";
        };
    }

    /**
     * 获取阅读量Top小说
     */
    public List<Object[]> getTopRead() {
        String sql = "SELECT * FROM TopRead";
        return hiveService.query(sql);
    }

    /**
     * 获取打赏Top小说
     */
    public List<Object[]> getTopReward() {
        String sql = "SELECT * FROM TopReward";
        return hiveService.query(sql);
    }

    /**
     * 获取类型平均阅读量
     */
    public List<Object[]> getTypeAvgRead() {
        String sql = "SELECT * FROM typeAvgRead";
        return hiveService.query(sql);
    }

    /**
     * 获取月票Top
     */
    public List<Object[]> getTopMTicket() {
        String sql = "SELECT * FROM TopMTicket";
        return hiveService.query(sql);
    }

    /**
     * 获取小说类型最大阅读量和收藏量
     */
    public List<Object[]> getNovelTypeMaxRF() {
        String sql = "SELECT * FROM novelTypeMaxRF";
        return hiveService.query(sql);
    }

    /**
     * 获取小说最大分享
     */
    public List<Object[]> getNovelMaxShare() {
        String sql = "SELECT * FROM novelMaxShare";
        return hiveService.query(sql);
    }

    /**
     * 获取小说类型统计
     */
    public List<Object[]> getNovelType() {
        String sql = "SELECT * FROM novelType";
        return hiveService.query(sql);
    }

    /**
     * 获取小说阅读范围分布
     */
    public List<Object[]> getNovelReadRange() {
        String sql = "SELECT * FROM novelReadRange";
        return hiveService.query(sql);
    }

    /**
     * 获取小说字数范围分布
     */
    public List<Object[]> getNovelWordNumRange() {
        String sql = "SELECT * FROM novelWordNumRange";
        return hiveService.query(sql);
    }

    /**
     * 获取小说鲜花Top
     */
    public List<Object[]> getNovelFlowerTop() {
        String sql = "SELECT * FROM novelFlowerTop";
        return hiveService.query(sql);
    }

    /**
     * 获取作者日更Top
     */
    public List<Object[]> getAuthorDayTop() {
        String sql = "SELECT * FROM authorDayTop";
        return hiveService.query(sql);
    }

    /**
     * 获取作者最大字数（Top10）
     */
    public List<Object[]> getAuthorMaxWord() {
        String sql = "SELECT * FROM authorMaxWord";
        return hiveService.query(sql);
    }

    /**
     * 获取小说月度数量统计
     */
    public List<Object[]> getNovelMonthCount() {
        String sql = "SELECT * FROM novelMonthCount";
        return hiveService.query(sql);
    }

    /**
     * 获取小说月度阅读统计
     */
    public List<Object[]> getNovelMonthRead() {
        String sql = "SELECT * FROM novelMonthRead";
        return hiveService.query(sql);
    }

    /**
     * 获取小说年度阅读统计
     */
    public List<Object[]> getNovelYearRead() {
        String sql = "SELECT * FROM novelYearRead";
        return hiveService.query(sql);
    }

    // ==============================
    // novelHot表查询方法 - 频道对比分析
    // ==============================

    /**
     * 获取频道主题分布数据
     */
    public List<Object[]> getChannelThemeDistribution() {
        String sql = """
                SELECT
                    channel,
                    type,
                    COUNT(*) as novel_count,
                    AVG(allRead) as avg_read,
                    AVG(allFlower) as avg_flower,
                    AVG(hotScore) as avg_hot_score
                FROM novelHot
                WHERE channel IN ('男频', '女频')
                GROUP BY channel, type
                ORDER BY channel, novel_count DESC
                """;
        return hiveService.query(sql);
    }

    /**
     * 获取频道收藏趋势数据
     */
    public List<Object[]> getChannelFlowerTrend() {
        String sql = """
                SELECT
                    channel,
                    CASE
                        WHEN allFlower < 1000 THEN '低收藏(<1000)'
                        WHEN allFlower < 5000 THEN '中收藏(1000-5000)'
                        WHEN allFlower < 10000 THEN '高收藏(5000-10000)'
                        ELSE '超高收藏(>10000)'
                    END as flower_level,
                    COUNT(*) as novel_count,
                    AVG(allRead) as avg_read,
                    AVG(hotScore) as avg_hot_score
                FROM novelHot
                WHERE channel IN ('男频', '女频')
                GROUP BY channel, flower_level
                ORDER BY channel, novel_count DESC
                """;
        return hiveService.query(sql);
    }

    /**
     * 获取频道TOP作品表现数据
     */
    public List<Object[]> getChannelTopPerformance() {
        String sql = """
                SELECT
                    channel,
                    type,
                    title,
                    allRead,
                    allFlower,
                    hotScore,
                    ROW_NUMBER() OVER (PARTITION BY channel ORDER BY hotScore DESC) as rank_in_channel
                FROM novelHot
                WHERE channel IN ('男频', '女频')
                QUALIFY rank_in_channel <= 10
                ORDER BY channel, hotScore DESC
                """;
        return hiveService.query(sql);
    }

    /**
     * 获取频道综合统计数据
     */
    public List<Object[]> getChannelComparisonStats() {
        String sql = """
                SELECT
                    channel,
                    COUNT(*) as novel_count,
                    AVG(wordNum) as avg_word_num,
                    AVG(allRead) as avg_read,
                    AVG(allFlower) as avg_flower,
                    AVG(reward) as avg_reward,
                    AVG(hotScore) as avg_hot_score,
                    MAX(wordNum) as max_word_num,
                    MAX(allRead) as max_read,
                    MAX(allFlower) as max_flower,
                    MAX(reward) as max_reward,
                    MAX(hotScore) as max_hot_score
                FROM novelHot
                GROUP BY channel
                ORDER BY channel
                """;
        return hiveService.query(sql);
    }

    /**
     * 获取主题市场表现数据
     */
    public List<Object[]> getThemeMarketPerformance() {
        String sql = """
                SELECT
                    type,
                    COUNT(*) as novel_count,
                    AVG(allRead) as avg_read,
                    AVG(allFlower) as avg_flower,
                    AVG(hotScore) as avg_hot_score,
                    (AVG(allRead) * 0.4 + AVG(allFlower) * 0.3 + AVG(hotScore) * 0.3) as theme_popularity_index
                FROM novelHot
                GROUP BY type
                HAVING COUNT(*) >= 5
                ORDER BY theme_popularity_index DESC
                """;
        return hiveService.query(sql);
    }

    /**
     * 获取主题受欢迎程度数据
     */
    public List<Object[]> getThemePopularity() {
        String sql = """
                SELECT
                    type,
                    channel,
                    COUNT(*) as novel_count,
                    AVG(allFlower) as avg_flower,
                    AVG(allRead) as avg_read,
                    AVG(hotScore) as avg_hot_score
                FROM novelHot
                WHERE channel IN ('男频', '女频')
                GROUP BY type, channel
                HAVING COUNT(*) >= 3
                ORDER BY type, channel
                """;
        return hiveService.query(sql);
    }

    /**
     * 获取主题字数分布数据
     */
    public List<Object[]> getThemeWordLengthDistribution() {
        String sql = """
                SELECT
                    type,
                    CASE
                        WHEN wordNum < 100000 THEN '短篇(<10万)'
                        WHEN wordNum < 500000 THEN '中篇(10-50万)'
                        WHEN wordNum < 1000000 THEN '长篇(50-100万)'
                        ELSE '超长篇(>100万)'
                    END as word_length_category,
                    COUNT(*) as novel_count,
                    AVG(allRead) as avg_read,
                    AVG(allFlower) as avg_flower
                FROM novelHot
                GROUP BY type, word_length_category
                HAVING COUNT(*) >= 2
                ORDER BY type,
                    CASE word_length_category
                        WHEN '短篇(<10万)' THEN 1
                        WHEN '中篇(10-50万)' THEN 2
                        WHEN '长篇(50-100万)' THEN 3
                        ELSE 4
                    END
                """;
        return hiveService.query(sql);
    }

    /**
     * 按频道和类型查询数据
     */
    public List<Object[]> getChannelDataByType(String channel, String type) {
        String sql = """
                SELECT *
                FROM novelHot
                WHERE channel = ? AND type = ?
                ORDER BY hotScore DESC
                """;
        return hiveService.query(sql, channel, type);
    }

    /**
     * 按指标获取热门主题
     */
    public List<Object[]> getTopThemesByMetric(String metric, int limit) {
        String validMetric = switch (metric.toLowerCase()) {
            case "avg_read" -> "AVG(allRead)";
            case "avg_flower" -> "AVG(allFlower)";
            case "avg_hotscore" -> "AVG(hotScore)";
            case "novel_count" -> "COUNT(*)";
            default -> "AVG(allRead)";
        };

        String sql = String.format("""
                SELECT
                    type,
                    %s as metric_value
                FROM novelHot
                GROUP BY type
                ORDER BY metric_value DESC
                LIMIT %d
                """, validMetric, limit);

        return hiveService.query(sql);
    }

    // ==============================
    // authorprofile表查询方法 - 作者分析
    // ==============================

    /**
     * 获取作者等级分布数据
     */
    public List<Object[]> getAuthorLevelDistribution() {
        String sql = """
                SELECT
                    channel,
                    level,
                    COUNT(*) as author_count,
                    AVG(workCount) as avg_work_count,
                    AVG(totalRead) as avg_total_read,
                    AVG(totalFlower) as avg_total_flower
                FROM authorprofile
                WHERE channel IN ('男频', '女频') AND level IS NOT NULL
                GROUP BY channel, level
                ORDER BY channel, level
                """;
        return hiveService.query(sql);
    }

    /**
     * 获取作者生产力分析数据
     */
    public List<Object[]> getAuthorProductivityAnalysis() {
        String sql = """
                SELECT
                    author,
                    workCount,
                    authorWords,
                    authorDays,
                    ROUND(authorWords / NULLIF(authorDays, 0), 0) as words_per_day,
                    ROUND(authorWords / NULLIF(workCount, 0), 0) as avg_words_per_work
                FROM authorprofile
                WHERE authorDays > 0 AND workCount > 0
                ORDER BY workCount DESC
                LIMIT 50
                """;
        return hiveService.query(sql);
    }

    /**
     * 获取作者阅读收藏分析数据
     */
    public List<Object[]> getAuthorReadFlowerAnalysis() {
        String sql = """
                SELECT
                    author,
                    totalRead,
                    totalFlower,
                    ROUND(totalFlower / NULLIF(totalRead, 0) * 1000, 2) as flower_ratio_per_thousand,
                    channel,
                    workCount
                FROM authorprofile
                WHERE totalRead > 0
                ORDER BY totalRead DESC
                LIMIT 100
                """;
        return hiveService.query(sql);
    }

    /**
     * 获取频道作者对比统计数据
     */
    public List<Object[]> getChannelAuthorComparisonStats() {
        String sql = """
                SELECT
                    channel,
                    COUNT(*) as author_count,
                    AVG(workCount) as avg_work_count,
                    AVG(totalRead) as avg_total_read,
                    AVG(totalFlower) as avg_total_flower,
                    AVG(authorWords) as avg_author_words,
                    AVG(authorDays) as avg_author_days,
                    MAX(workCount) as max_work_count,
                    MAX(totalRead) as max_total_read,
                    MAX(totalFlower) as max_total_flower
                FROM authorprofile
                WHERE channel IN ('男频', '女频')
                GROUP BY channel
                ORDER BY channel
                """;
        return hiveService.query(sql);
    }

    /**
     * 获取高产作者TOP榜数据
     */
    public List<Object[]> getTopAuthorsByMetric(String metric, int limit) {
        String validMetric = switch (metric.toLowerCase()) {
            case "workcount" -> "workCount";
            case "totalread" -> "totalRead";
            case "totalflower" -> "totalFlower";
            case "authorwords" -> "authorWords";
            default -> "workCount";
        };

        String sql = String.format("""
                SELECT
                    author,
                    %s as metric_value,
                    channel,
                    workCount,
                    totalRead,
                    totalFlower
                FROM authorprofile
                ORDER BY metric_value DESC
                LIMIT %d
                """, validMetric, limit);

        return hiveService.query(sql);
    }

    /**
     * 获取不同等级作者的创作天数分析数据
     */
    public List<Object[]> getAuthorDaysByLevel() {
        String sql = """
                SELECT
                    channel,
                    level,
                    COUNT(*) as author_count,
                    AVG(authorDays) as avg_author_days,
                    MIN(authorDays) as min_author_days,
                    MAX(authorDays) as max_author_days
                FROM authorprofile
                WHERE channel IN ('男频', '女频') AND level IS NOT NULL AND authorDays > 0
                GROUP BY channel, level
                ORDER BY channel, level
                """;
        return hiveService.query(sql);
    }

    /**
     * 获取作者创作效率分析数据
     */
    public List<Object[]> getAuthorEfficiencyAnalysis() {
        String sql = """
                SELECT
                    channel,
                    CASE
                        WHEN workCount <= 2 THEN '新手作者'
                        WHEN workCount <= 4 THEN '成长作者'
                        WHEN workCount <= 6 THEN '成熟作者'
                        ELSE '高产作者'
                    END as experience_level,
                    COUNT(*) as author_count,
                    AVG(totalRead) as avg_total_read,
                    AVG(totalFlower) as avg_total_flower,
                    AVG(authorWords) as avg_author_words
                FROM authorprofile
                WHERE channel IN ('男频', '女频')
                GROUP BY channel, experience_level
                ORDER BY channel,
                    CASE experience_level
                        WHEN '新手作者' THEN 1
                        WHEN '成长作者' THEN 2
                        WHEN '成熟作者' THEN 3
                        ELSE 4
                    END
                """;
        return hiveService.query(sql);
    }
}