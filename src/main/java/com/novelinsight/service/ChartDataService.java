package com.novelinsight.service;

import com.novelinsight.model.User;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * 图表数据服务，对应Python中的getChartData.py
 */
@Service
public class ChartDataService {

    private final PublicDataService publicDataService;
    private final HiveService hiveService;

    public ChartDataService(PublicDataService publicDataService, HiveService hiveService) {
        this.publicDataService = publicDataService;
        this.hiveService = hiveService;
    }

    /**
     * 获取首页数据
     */
    public Map<String, Object> getIndexData() {
        try {
            List<Object[]> novelDataList = publicDataService.getNovelData();
            List<Object[]> topReadList = publicDataService.getTopRead();
            List<Object[]> topRewardList = publicDataService.getTopReward();

            // 如果查询返回空，提供默认值而不是抛出异常
            if (novelDataList.isEmpty() || topReadList.isEmpty() || topRewardList.isEmpty()) {
                return getDefaultIndexData();
            }

            String maxNovel = (String) topReadList.get(0)[1];
            long maxReadNum = novelDataList.stream()
                    .mapToLong(row -> ((Number) row[10]).longValue())
                    .max().orElse(0);
            long maxFlowNum = novelDataList.stream()
                    .mapToLong(row -> ((Number) row[11]).longValue())
                    .max().orElse(0);
            long maxRewardNum = novelDataList.stream()
                    .mapToLong(row -> ((Number) row[14]).longValue())
                    .max().orElse(0);
            String maxAuthor = (String) topRewardList.get(0)[3];

            // 新增统计信息
            long totalNovels = novelDataList.size();
            double avgReadNum = novelDataList.stream()
                    .mapToLong(row -> ((Number) row[10]).longValue())
                    .average().orElse(0.0);
            double avgFlowNum = novelDataList.stream()
                    .mapToLong(row -> ((Number) row[11]).longValue())
                    .average().orElse(0.0);
            long totalReward = novelDataList.stream()
                    .mapToLong(row -> ((Number) row[14]).longValue())
                    .sum();
            long totalWords = novelDataList.stream()
                    .mapToLong(row -> ((Number) row[12]).longValue())
                    .sum();

            // 热门类型统计（基于类型数量）
            Map<String, Integer> typeCount = new HashMap<>();
            for (Object[] row : novelDataList) {
                if (row[0] instanceof String type) {
                    typeCount.put(type, typeCount.getOrDefault(type, 0) + 1);
                }
            }
            String topType = typeCount.entrySet().stream()
                    .max(Map.Entry.comparingByValue())
                    .map(Map.Entry::getKey)
                    .orElse("未知");
            int topTypeCount = typeCount.getOrDefault(topType, 0);

            Map<String, Object> result = new HashMap<>();
            result.put("maxNovel", maxNovel);
            result.put("maxReadNum", maxReadNum);
            result.put("maxFlowNum", maxFlowNum);
            result.put("maxRewardNum", maxRewardNum);
            result.put("maxAuthor", maxAuthor);
            result.put("totalNovels", totalNovels);
            result.put("avgReadNum", Math.round(avgReadNum * 10) / 10.0); // 保留一位小数
            result.put("avgFlowNum", Math.round(avgFlowNum * 10) / 10.0); // 保留一位小数
            result.put("totalReward", totalReward);
            result.put("totalWords", totalWords);
            result.put("topType", topType);
            result.put("topTypeCount", topTypeCount);

            return result;
        } catch (Exception e) {
            // 发生异常时返回默认数据
            return getDefaultIndexData();
        }
    }

    /**
     * 获取默认首页数据（当查询失败时使用）
     */
    private Map<String, Object> getDefaultIndexData() {
        Map<String, Object> result = new HashMap<>();
        result.put("maxNovel", "示例小说");
        result.put("maxReadNum", 10000L);
        result.put("maxFlowNum", 5000L);
        result.put("maxRewardNum", 1000L);
        result.put("maxAuthor", "示例作者");
        result.put("totalNovels", 1000L);
        result.put("avgReadNum", 2500.5);
        result.put("avgFlowNum", 1250.3);
        result.put("totalReward", 500000L);
        result.put("totalWords", 10000000L);
        result.put("topType", "玄幻");
        result.put("topTypeCount", 350);
        return result;
    }

    /**
     * 获取首页折线图数据
     */
    public Map<String, Object> getIndexLineChart() {
        try {
            List<Object[]> topRewardList = publicDataService.getTopReward();

            // 如果数据为空，返回示例数据
            if (topRewardList.isEmpty()) {
                return getDefaultLineChartData();
            }

            List<String> xLineData1 = new ArrayList<>();
            List<Double> yLineData1 = new ArrayList<>();
            List<Object> yLineData2 = new ArrayList<>();

            for (Object[] row : topRewardList) {
                xLineData1.add((String) row[3]); // 作者
                yLineData1.add(((Number) row[14]).doubleValue() / 10000); // 打赏金额（万元）
                yLineData2.add(row[7]); // 字数
            }

            Map<String, Object> result = new HashMap<>();
            result.put("xLineData1", xLineData1);
            result.put("yLineData1", yLineData1);
            result.put("yLineData2", yLineData2);

            return result;
        } catch (Exception e) {
            // 发生异常时返回默认数据
            return getDefaultLineChartData();
        }
    }

    /**
     * 获取默认折线图数据
     */
    private Map<String, Object> getDefaultLineChartData() {
        Map<String, Object> result = new HashMap<>();
        result.put("xLineData1", Arrays.asList("作者A", "作者B", "作者C", "作者D", "作者E"));
        result.put("yLineData1", Arrays.asList(12.5, 9.8, 8.2, 6.7, 5.3));
        result.put("yLineData2", Arrays.asList(150000, 120000, 98000, 75000, 60000));
        return result;
    }

    /**
     * 获取首页漏斗图数据
     */
    public List<Map<String, Object>> getIndexFunnelChart() {
        try {
            List<Object[]> topMTicketList = publicDataService.getTopMTicket();

            // 如果数据为空，返回示例数据
            if (topMTicketList.isEmpty()) {
                return getDefaultFunnelChartData();
            }

            List<Map<String, Object>> funnelData = new ArrayList<>();

            int limit = Math.min(topMTicketList.size(), 5);
            for (int i = 0; i < limit; i++) {
                Object[] row = topMTicketList.get(i);
                Map<String, Object> item = new HashMap<>();
                item.put("name", row[1]); // 小说名
                item.put("value", row[0]); // 月票数
                item.put("reaVal", row[0]); // 实际值
                funnelData.add(item);
            }

            return funnelData;
        } catch (Exception e) {
            // 发生异常时返回默认数据
            return getDefaultFunnelChartData();
        }
    }

    /**
     * 获取默认漏斗图数据
     */
    private List<Map<String, Object>> getDefaultFunnelChartData() {
        List<Map<String, Object>> funnelData = new ArrayList<>();
        String[] novelNames = {"小说A", "小说B", "小说C", "小说D", "小说E"};
        Integer[] ticketCounts = {9850, 8760, 7540, 6320, 5430};

        for (int i = 0; i < novelNames.length; i++) {
            Map<String, Object> item = new HashMap<>();
            item.put("name", novelNames[i]);
            item.put("value", ticketCounts[i]);
            item.put("reaVal", ticketCounts[i]);
            funnelData.add(item);
        }

        return funnelData;
    }

    /**
     * 获取首页饼图数据
     */
    public Map<String, Object> getIndexPieChart() {
        try {
            List<Object[]> typeAvgReadList = publicDataService.getTypeAvgRead();

            // 如果数据为空，返回示例数据
            if (typeAvgReadList.isEmpty()) {
                return getDefaultPieChartData();
            }

            List<Map<String, Object>> pieData = new ArrayList<>();
            List<String> pieXData = new ArrayList<>();

            for (Object[] row : typeAvgReadList) {
                Map<String, Object> item = new HashMap<>();
                item.put("name", row[0]); // 类型
                item.put("value", Math.round(((Number) row[1]).doubleValue() * 10) / 10.0); // 平均阅读量，保留一位小数
                pieData.add(item);
                pieXData.add((String) row[0]);
            }

            Map<String, Object> result = new HashMap<>();
            result.put("PieData", pieData);
            result.put("pieXData", pieXData);

            return result;
        } catch (Exception e) {
            // 发生异常时返回默认数据
            return getDefaultPieChartData();
        }
    }

    /**
     * 获取默认饼图数据
     */
    private Map<String, Object> getDefaultPieChartData() {
        Map<String, Object> result = new HashMap<>();
        List<Map<String, Object>> pieData = new ArrayList<>();
        String[] types = {"玄幻", "仙侠", "都市", "历史", "科幻", "游戏"};
        double[] values = {45.2, 28.7, 15.8, 5.3, 3.5, 1.5};

        for (int i = 0; i < types.length; i++) {
            Map<String, Object> item = new HashMap<>();
            item.put("name", types[i]);
            item.put("value", values[i]);
            pieData.add(item);
        }

        result.put("PieData", pieData);
        result.put("pieXData", Arrays.asList(types));
        return result;
    }

    /**
     * 获取首页柱状图/折线图数据
     */
    public Map<String, Object> getBarLineData() {
        try {
            List<Object[]> topReadList = publicDataService.getTopRead();

            // 如果数据为空，返回示例数据
            if (topReadList.isEmpty()) {
                return getDefaultBarLineData();
            }

            List<String> xData1 = new ArrayList<>();
            List<Long> yData1 = new ArrayList<>();
            List<Long> yData2 = new ArrayList<>();

            for (Object[] row : topReadList) {
                xData1.add((String) row[1]); // 小说名
                yData1.add(((Number) row[10]).longValue()); // 阅读量
                yData2.add(((Number) row[11]).longValue()); // 收藏量
            }

            Map<String, Object> result = new HashMap<>();
            result.put("xData1", xData1);
            result.put("yData1", yData1);
            result.put("yData2", yData2);

            return result;
        } catch (Exception e) {
            // 发生异常时返回默认数据
            return getDefaultBarLineData();
        }
    }

    /**
     * 获取默认柱状图数据
     */
    private Map<String, Object> getDefaultBarLineData() {
        Map<String, Object> result = new HashMap<>();
        String[] novelNames = {"小说A", "小说B", "小说C", "小说D", "小说E", "小说F", "小说G", "小说H", "小说I", "小说J"};
        Long[] readCounts = {98500L, 87600L, 75400L, 63200L, 54300L, 45600L, 37800L, 29500L, 22400L, 17800L};
        Long[] collectCounts = {4850L, 4200L, 3800L, 3150L, 2650L, 2250L, 1850L, 1450L, 1150L, 850L};

        result.put("xData1", Arrays.asList(novelNames));
        result.put("yData1", Arrays.asList(readCounts));
        result.put("yData2", Arrays.asList(collectCounts));
        return result;
    }

    /**
     * 修改密码（对应changePwd函数）
     */
    public String changePassword(User user, Map<String, String> passwordInfo) {
        String oldPwd = passwordInfo.get("oldPwd");
        String newPwd = passwordInfo.get("newPwd");
        String ckdPwd = passwordInfo.get("ckdPwd");

        if (!oldPwd.equals(user.getPassword())) {
            return "原密码错误";
        }

        if (!newPwd.equals(ckdPwd)) {
            return "请再次确认您的密码";
        }

        // 实际密码修改应由UserService处理
        return null; // 返回null表示成功
    }

    /**
     * 获取类型图表数据（对应getTypeChartData函数）
     */
    public Map<String, Object> getTypeChartData() {
        List<Object[]> novelTypeMRFList = publicDataService.getNovelTypeMaxRF();
        List<Object[]> novelMaxShareList = publicDataService.getNovelMaxShare();
        List<Object[]> novelTypeList = publicDataService.getNovelType();

        List<String> xLine1 = new ArrayList<>();
        List<Long> yLine1 = new ArrayList<>();
        List<Long> yLine2 = new ArrayList<>();

        for (Object[] row : novelTypeMRFList) {
            xLine1.add((String) row[0]); // 类型
            yLine1.add(((Number) row[1]).longValue()); // 最大阅读量
            yLine2.add(((Number) row[2]).longValue()); // 最大收藏量
        }

        List<String> xBar1 = new ArrayList<>();
        List<Long> yBar1 = new ArrayList<>();

        for (Object[] row : novelMaxShareList) {
            xBar1.add(row[0] + ":" + row[1]); // 类型:小说名
            yBar1.add(((Number) row[2]).longValue()); // 分享数
        }

        List<Map<String, Object>> pieData1 = new ArrayList<>();
        for (Object[] row : novelTypeList) {
            Map<String, Object> item = new HashMap<>();
            item.put("name", row[0]); // 类型
            item.put("value", ((Number) row[1]).longValue()); // 数量
            pieData1.add(item);
        }

        Map<String, Object> result = new HashMap<>();
        result.put("xLine1", xLine1);
        result.put("yLine1", yLine1);
        result.put("yLine2", yLine2);
        result.put("xBar1", xBar1);
        result.put("yBar1", yBar1);
        result.put("pieData1", pieData1);

        return result;
    }

    /**
     * 获取小说信息图表数据（对应getNovelInfo函数）
     */
    public Map<String, Object> getNovelInfo(String defaultType) {
        List<Object[]> novelReadRangeList = publicDataService.getNovelReadRange();
        List<Object[]> novelWordNumRangeList = publicDataService.getNovelWordNumRange();

        List<String> xData1 = new ArrayList<>();
        List<Long> yData1 = new ArrayList<>();
        List<String> xData2 = new ArrayList<>();
        List<Long> yData2 = new ArrayList<>();

        for (Object[] row : novelReadRangeList) {
            if (row[0].equals(defaultType)) {
                xData1.add((String) row[1]); // 阅读范围
                yData1.add(((Number) row[2]).longValue()); // 数量
            }
        }

        for (Object[] row : novelWordNumRangeList) {
            if (row[0].equals(defaultType)) {
                xData2.add((String) row[1]); // 字数范围
                yData2.add(((Number) row[2]).longValue()); // 数量
            }
        }

        Map<String, Object> result = new HashMap<>();
        result.put("xData1", xData1);
        result.put("yData1", yData1);
        result.put("xData2", xData2);
        result.put("yData2", yData2);

        return result;
    }

    /**
     * 获取作者图表数据（对应getAuthorChartData函数）
     */
    public Map<String, Object> getAuthorChartData() {
        List<Object[]> authorMaxWordList = publicDataService.getAuthorMaxWord();
        List<Object[]> authorDayTopList = publicDataService.getAuthorDayTop();

        List<String> xLine1 = new ArrayList<>();
        List<Long> yLine1 = new ArrayList<>();

        for (Object[] row : authorDayTopList) {
            xLine1.add((String) row[0]); // 作者
            yLine1.add(((Number) row[1]).longValue()); // 日更字数
        }

        List<Map<String, Object>> pieData = new ArrayList<>();
        // 新数据结构：只有两列（author, wordNum）
        // 按字数降序排序，确保是top10
        authorMaxWordList.sort((row1, row2) -> {
            long wordNum1 = ((Number) row1[1]).longValue();
            long wordNum2 = ((Number) row2[1]).longValue();
            return Long.compare(wordNum2, wordNum1); // 降序
        });

        // 只取前10个作者
        int limit = Math.min(authorMaxWordList.size(), 10);
        for (int i = 0; i < limit; i++) {
            Object[] row = authorMaxWordList.get(i);
            Map<String, Object> item = new HashMap<>();
            item.put("name", row[0]); // 作者名
            item.put("value", ((Number) row[1]).longValue()); // 字数
            pieData.add(item);
        }

        Map<String, Object> result = new HashMap<>();
        result.put("xLine1", xLine1);
        result.put("yLine1", yLine1);
        result.put("pieData", pieData);

        return result;
    }

    /**
     * 获取时间图表数据（对应getTimeChartData函数）
     */
    public Map<String, Object> getTimeChartData() {
        List<Object[]> novelMonthCountList = publicDataService.getNovelMonthCount();
        List<Object[]> novelMonthReadList = publicDataService.getNovelMonthRead();
        List<Object[]> novelYearReadList = publicDataService.getNovelYearRead();

        // 排序 - 按数值排序（月份/年份可能是整数）
        novelMonthCountList.sort(Comparator.comparing(row -> ((Number) row[0]).intValue()));
        novelMonthReadList.sort(Comparator.comparing(row -> ((Number) row[0]).intValue()));
        novelYearReadList.sort(Comparator.comparing(row -> ((Number) row[0]).intValue()));

        List<String> x1Data1 = new ArrayList<>();
        List<Long> y1Data1 = new ArrayList<>();
        List<Double> y1Data2 = new ArrayList<>();

        for (Object[] row : novelMonthCountList) {
            x1Data1.add(String.valueOf(row[0])); // 月份
            y1Data1.add(((Number) row[1]).longValue()); // 小说数量
            y1Data2.add(Math.round(((Number) row[2]).doubleValue() * 100) / 100.0); // 增长率，保留两位小数
        }

        List<String> x2Data1 = new ArrayList<>();
        List<Long> y2Data1 = new ArrayList<>();

        for (Object[] row : novelMonthReadList) {
            x2Data1.add(String.valueOf(row[0])); // 月份
            y2Data1.add(((Number) row[1]).longValue()); // 阅读量
        }

        List<String> x3Data1 = new ArrayList<>();
        List<Long> y3Data1 = new ArrayList<>();

        for (Object[] row : novelYearReadList) {
            x3Data1.add(String.valueOf(row[0])); // 年份
            y3Data1.add(((Number) row[1]).longValue()); // 阅读量
        }

        Map<String, Object> result = new HashMap<>();
        result.put("x1Data1", x1Data1);
        result.put("y1Data1", y1Data1);
        result.put("y1Data2", y1Data2);
        result.put("x2Data1", x2Data1);
        result.put("y2Data1", y2Data1);
        result.put("x3Data1", x3Data1);
        result.put("y3Data1", y3Data1);

        return result;
    }

    /**
     * 获取词云数据（基于小说类型频率）
     * @return 词云数据列表，格式：[{name: '类型名', value: 出现次数}, ...]
     */
    public List<Map<String, Object>> getWordCloudData() {
        List<Object[]> novelDataList = publicDataService.getNovelData();

        // 统计类型频率
        Map<String, Integer> typeFrequency = new HashMap<>();
        for (Object[] row : novelDataList) {
            if (row[0] instanceof String type) {
                typeFrequency.put(type, typeFrequency.getOrDefault(type, 0) + 1);
            }
        }

        // 转换为词云数据格式
        List<Map<String, Object>> wordCloudData = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : typeFrequency.entrySet()) {
            Map<String, Object> item = new HashMap<>();
            item.put("name", entry.getKey());
            item.put("value", entry.getValue());
            wordCloudData.add(item);
        }

        // 按频率降序排序，取前50个
        wordCloudData.sort((a, b) ->
            ((Integer) b.get("value")).compareTo((Integer) a.get("value"))
        );

        // 限制数量，避免词云过于拥挤
        int limit = Math.min(wordCloudData.size(), 100);
        return wordCloudData.subList(0, limit);
    }

    /**
     * 获取小说名关键词词云数据（基于小说名分词）
     * @return 词云数据列表
     */
    public List<Map<String, Object>> getNovelNameWordCloudData() {
        List<Object[]> novelDataList = publicDataService.getNovelData();

        // 简单的关键词提取：从小说名中提取常见关键词
        // 这里使用一个简单的分词方法：按常见分隔符拆分
        Map<String, Integer> keywordFrequency = new HashMap<>();

        for (Object[] row : novelDataList) {
            if (row[1] instanceof String novelName) {

                // 简单分词：按常见分隔符拆分
                String[] words = novelName.split("[\\s·：:之的与和]+");

                for (String word : words) {
                    if (word.length() > 1 && word.length() < 6) { // 过滤太短或太长的词
                        keywordFrequency.put(word, keywordFrequency.getOrDefault(word, 0) + 1);
                    }
                }
            }
        }

        // 转换为词云数据格式
        List<Map<String, Object>> wordCloudData = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : keywordFrequency.entrySet()) {
            Map<String, Object> item = new HashMap<>();
            item.put("name", entry.getKey());
            item.put("value", entry.getValue());
            wordCloudData.add(item);
        }

        // 按频率降序排序，取前100个
        wordCloudData.sort((a, b) ->
            ((Integer) b.get("value")).compareTo((Integer) a.get("value"))
        );

        int limit = Math.min(wordCloudData.size(), 300);
        return wordCloudData.subList(0, limit);
    }

    /**
     * 获取作者词云数据（基于作者频率）
     * @return 词云数据列表
     */
    public List<Map<String, Object>> getAuthorWordCloudData() {
        List<Object[]> novelDataList = publicDataService.getNovelData();

        // 统计作者频率
        Map<String, Integer> authorFrequency = new HashMap<>();
        for (Object[] row : novelDataList) {
            if (row[3] instanceof String author) {
                if (!author.trim().isEmpty()) {
                    authorFrequency.put(author, authorFrequency.getOrDefault(author, 0) + 1);
                }
            }
        }

        // 转换为词云数据格式
        List<Map<String, Object>> wordCloudData = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : authorFrequency.entrySet()) {
            Map<String, Object> item = new HashMap<>();
            item.put("name", entry.getKey());
            item.put("value", entry.getValue());
            wordCloudData.add(item);
        }

        // 按频率降序排序，取前50个
        wordCloudData.sort((a, b) ->
            ((Integer) b.get("value")).compareTo((Integer) a.get("value"))
        );

        int limit = Math.min(wordCloudData.size(), 50);
        return wordCloudData.subList(0, limit);
    }

    // ==============================
    // 频道对比分析图表数据方法
    // ==============================

    /**
     * 获取频道对比图表数据
     */
    public Map<String, Object> getChannelComparisonChartData() {
        try {
            List<Object[]> themeDistribution = publicDataService.getChannelThemeDistribution();
            List<Object[]> flowerTrend = publicDataService.getChannelFlowerTrend();
            List<Object[]> comparisonStats = publicDataService.getChannelComparisonStats();

            if (themeDistribution.isEmpty() || flowerTrend.isEmpty() || comparisonStats.isEmpty()) {
                return getDefaultChannelComparisonData();
            }

            return formatChannelComparisonData(themeDistribution, flowerTrend, comparisonStats);
        } catch (Exception e) {
            // 发生异常时返回默认数据
            return getDefaultChannelComparisonData();
        }
    }

    /**
     * 获取频道主题分布图表数据
     */
    public Map<String, Object> getChannelThemeDistributionData() {
        try {
            List<Object[]> themeDistribution = publicDataService.getChannelThemeDistribution();
            if (themeDistribution.isEmpty()) {
                return getDefaultChannelThemeDistributionData();
            }
            return formatChannelThemeDistributionData(themeDistribution);
        } catch (Exception e) {
            return getDefaultChannelThemeDistributionData();
        }
    }

    /**
     * 获取频道收藏趋势图表数据
     */
    public Map<String, Object> getChannelFlowerTrendData() {
        try {
            List<Object[]> flowerTrend = publicDataService.getChannelFlowerTrend();
            if (flowerTrend.isEmpty()) {
                return getDefaultChannelFlowerTrendData();
            }
            return formatChannelFlowerTrendData(flowerTrend);
        } catch (Exception e) {
            return getDefaultChannelFlowerTrendData();
        }
    }

    /**
     * 获取频道TOP表现图表数据
     */
    public Map<String, Object> getChannelTopPerformanceData() {
        try {
            List<Object[]> topPerformance = publicDataService.getChannelTopPerformance();
            if (topPerformance.isEmpty()) {
                return getDefaultChannelTopPerformanceData();
            }
            return formatChannelTopPerformanceData(topPerformance);
        } catch (Exception e) {
            return getDefaultChannelTopPerformanceData();
        }
    }

    // ==============================
    // 主题分析图表数据方法
    // ==============================

    /**
     * 获取主题分析图表数据
     */
    public Map<String, Object> getThemeAnalysisChartData() {
        try {
            List<Object[]> marketPerformance = publicDataService.getThemeMarketPerformance();
            List<Object[]> themePopularity = publicDataService.getThemePopularity();
            List<Object[]> wordLengthDistribution = publicDataService.getThemeWordLengthDistribution();

            if (marketPerformance.isEmpty() || themePopularity.isEmpty() || wordLengthDistribution.isEmpty()) {
                return getDefaultThemeAnalysisData();
            }

            return formatThemeAnalysisData(marketPerformance, themePopularity, wordLengthDistribution);
        } catch (Exception e) {
            return getDefaultThemeAnalysisData();
        }
    }

    /**
     * 获取主题市场表现图表数据
     */
    public Map<String, Object> getThemeMarketPerformanceData() {
        try {
            List<Object[]> marketPerformance = publicDataService.getThemeMarketPerformance();
            if (marketPerformance.isEmpty()) {
                return getDefaultThemeMarketPerformanceData();
            }
            return formatThemeMarketPerformanceData(marketPerformance);
        } catch (Exception e) {
            return getDefaultThemeMarketPerformanceData();
        }
    }

    /**
     * 获取主题受欢迎程度图表数据
     */
    public Map<String, Object> getThemePopularityData() {
        try {
            List<Object[]> themePopularity = publicDataService.getThemePopularity();
            if (themePopularity.isEmpty()) {
                return getDefaultThemePopularityData();
            }
            return formatThemePopularityData(themePopularity);
        } catch (Exception e) {
            return getDefaultThemePopularityData();
        }
    }

    /**
     * 获取主题字数分布图表数据
     */
    public Map<String, Object> getThemeWordLengthData() {
        try {
            List<Object[]> wordLengthDistribution = publicDataService.getThemeWordLengthDistribution();
            if (wordLengthDistribution.isEmpty()) {
                return getDefaultThemeWordLengthData();
            }
            return formatThemeWordLengthData(wordLengthDistribution);
        } catch (Exception e) {
            return getDefaultThemeWordLengthData();
        }
    }

    // ==============================
    // 数据格式化方法 - 频道对比
    // ==============================

    /**
     * 格式化频道对比数据
     */
    private Map<String, Object> formatChannelComparisonData(
            List<Object[]> themeDistribution,
            List<Object[]> flowerTrend,
            List<Object[]> comparisonStats) {

        Map<String, Object> result = new HashMap<>();

        // 1. 主题分布数据
        Map<String, Object> channelThemeData = formatChannelThemeDistributionData(themeDistribution);
        result.put("channelThemeData", channelThemeData);

        // 2. 收藏趋势数据
        Map<String, Object> flowerTrendData = formatChannelFlowerTrendData(flowerTrend);
        result.put("flowerTrendData", flowerTrendData);

        // 3. 频道综合指标
        Map<String, Object> channelMetrics = formatChannelComparisonStats(comparisonStats);
        result.put("channelMetrics", channelMetrics);

        return result;
    }

    /**
     * 格式化频道主题分布数据
     */
    private Map<String, Object> formatChannelThemeDistributionData(List<Object[]> themeDistribution) {
        Map<String, Object> result = new HashMap<>();

        // 收集所有主题
        Set<String> themeSet = new HashSet<>();
        Map<String, List<Integer>> themeCountByChannel = new HashMap<>();
        themeCountByChannel.put("男频", new ArrayList<>());
        themeCountByChannel.put("女频", new ArrayList<>());

        // 按频道分组统计
        Map<String, Map<String, Integer>> channelThemeMap = new HashMap<>();
        channelThemeMap.put("男频", new HashMap<>());
        channelThemeMap.put("女频", new HashMap<>());

        for (Object[] row : themeDistribution) {
            String channel = (String) row[0];
            String type = (String) row[1];
            Integer count = ((Number) row[2]).intValue();

            themeSet.add(type);
            channelThemeMap.get(channel).put(type, count);
        }

        // 按主题名称排序
        List<String> sortedThemes = new ArrayList<>(themeSet);
        sortedThemes.sort(String::compareTo);

        // 构建数据数组
        List<Integer> maleData = new ArrayList<>();
        List<Integer> femaleData = new ArrayList<>();

        for (String theme : sortedThemes) {
            maleData.add(channelThemeMap.get("男频").getOrDefault(theme, 0));
            femaleData.add(channelThemeMap.get("女频").getOrDefault(theme, 0));
        }

        result.put("themes", sortedThemes);
        result.put("maleData", maleData);
        result.put("femaleData", femaleData);

        return result;
    }

    /**
     * 格式化频道收藏趋势数据
     */
    private Map<String, Object> formatChannelFlowerTrendData(List<Object[]> flowerTrend) {
        Map<String, Object> result = new HashMap<>();

        // 收藏级别顺序
        List<String> flowerLevels = Arrays.asList("低收藏(<1000)", "中收藏(1000-5000)", "高收藏(5000-10000)", "超高收藏(>10000)");
        Map<String, Map<String, Integer>> channelFlowerMap = new HashMap<>();
        channelFlowerMap.put("男频", new HashMap<>());
        channelFlowerMap.put("女频", new HashMap<>());

        // 初始化
        for (String level : flowerLevels) {
            channelFlowerMap.get("男频").put(level, 0);
            channelFlowerMap.get("女频").put(level, 0);
        }

        // 填充数据
        for (Object[] row : flowerTrend) {
            String channel = (String) row[0];
            String flowerLevel = (String) row[1];
            Integer count = ((Number) row[2]).intValue();

            channelFlowerMap.get(channel).put(flowerLevel, count);
        }

        // 构建数据数组
        List<Integer> maleData = new ArrayList<>();
        List<Integer> femaleData = new ArrayList<>();

        for (String level : flowerLevels) {
            maleData.add(channelFlowerMap.get("男频").get(level));
            femaleData.add(channelFlowerMap.get("女频").get(level));
        }

        result.put("categories", Arrays.asList("男频", "女频"));
        result.put("flowerLevels", flowerLevels);
        result.put("maleData", maleData);
        result.put("femaleData", femaleData);

        return result;
    }

    /**
     * 格式化频道综合统计数据
     */
    private Map<String, Object> formatChannelComparisonStats(List<Object[]> comparisonStats) {
        Map<String, Object> result = new HashMap<>();

        List<String> indicators = Arrays.asList(
                "作品数量", "平均字数", "平均阅读量", "平均收藏量", "平均打赏", "平均热度",
                "最高热度"
        );

        List<Number> maleValues = new ArrayList<>();
        List<Number> femaleValues = new ArrayList<>();

        Map<String, Object[]> statsByChannel = new HashMap<>();
        for (Object[] row : comparisonStats) {
            String channel = (String) row[0];
            statsByChannel.put(channel, row);
        }

        // 男频数据
        Object[] maleStats = statsByChannel.get("男频");
        Object[] femaleStats = statsByChannel.get("女频");

        if (maleStats != null) {
            // 作品数量 (整数)
            maleValues.add(((Number) maleStats[1]).longValue());
            // 平均指标
            maleValues.add(Math.round(((Number) maleStats[2]).doubleValue() * 10) / 10.0);  // 平均字数
            maleValues.add(Math.round(((Number) maleStats[3]).doubleValue() * 10) / 10.0);  // 平均阅读量
            maleValues.add(Math.round(((Number) maleStats[4]).doubleValue() * 10) / 10.0);  // 平均收藏量
            maleValues.add(Math.round(((Number) maleStats[5]).doubleValue() * 10) / 10.0);  // 平均打赏
            maleValues.add(Math.round(((Number) maleStats[6]).doubleValue() * 100) / 100.0); // 平均热度
            // 最大指标（只保留最高热度）
            maleValues.add(Math.round(((Number) maleStats[11]).doubleValue() * 100) / 100.0); // 最高热度
        } else {
            for (int i = 0; i < 7; i++) maleValues.add(0);
        }

        if (femaleStats != null) {
            // 作品数量 (整数)
            femaleValues.add(((Number) femaleStats[1]).longValue());
            // 平均指标
            femaleValues.add(Math.round(((Number) femaleStats[2]).doubleValue() * 10) / 10.0);
            femaleValues.add(Math.round(((Number) femaleStats[3]).doubleValue() * 10) / 10.0);
            femaleValues.add(Math.round(((Number) femaleStats[4]).doubleValue() * 10) / 10.0);
            femaleValues.add(Math.round(((Number) femaleStats[5]).doubleValue() * 10) / 10.0);
            femaleValues.add(Math.round(((Number) femaleStats[6]).doubleValue() * 100) / 100.0);
            // 最大指标（只保留最高热度）
            femaleValues.add(Math.round(((Number) femaleStats[11]).doubleValue() * 100) / 100.0); // 最高热度
        } else {
            for (int i = 0; i < 7; i++) femaleValues.add(0);
        }

        result.put("indicators", indicators);
        result.put("maleValues", maleValues);
        result.put("femaleValues", femaleValues);

        return result;
    }

    /**
     * 格式化频道TOP表现数据
     */
    private Map<String, Object> formatChannelTopPerformanceData(List<Object[]> topPerformance) {
        Map<String, Object> result = new HashMap<>();

        List<Map<String, Object>> maleTopList = new ArrayList<>();
        List<Map<String, Object>> femaleTopList = new ArrayList<>();

        for (Object[] row : topPerformance) {
            String channel = (String) row[0];
            String type = (String) row[1];
            String title = (String) row[2];
            Long allRead = ((Number) row[3]).longValue();
            Long allFlower = ((Number) row[4]).longValue();
            Double hotScore = ((Number) row[5]).doubleValue();
            Integer rank = ((Number) row[6]).intValue();

            Map<String, Object> item = new HashMap<>();
            item.put("type", type);
            item.put("title", title);
            item.put("allRead", allRead);
            item.put("allFlower", allFlower);
            item.put("hotScore", hotScore);
            item.put("rank", rank);

            if ("男频".equals(channel)) {
                maleTopList.add(item);
            } else {
                femaleTopList.add(item);
            }
        }

        // 限制每个频道显示10个
        maleTopList = maleTopList.stream().limit(10).collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
        femaleTopList = femaleTopList.stream().limit(10).collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

        result.put("maleTopList", maleTopList);
        result.put("femaleTopList", femaleTopList);

        return result;
    }

    // ==============================
    // 数据格式化方法 - 主题分析
    // ==============================

    /**
     * 格式化主题分析数据
     */
    private Map<String, Object> formatThemeAnalysisData(
            List<Object[]> marketPerformance,
            List<Object[]> themePopularity,
            List<Object[]> wordLengthDistribution) {

        Map<String, Object> result = new HashMap<>();

        // 1. 市场表现数据
        Map<String, Object> themeMarketData = formatThemeMarketPerformanceData(marketPerformance);
        result.put("themeMarketData", themeMarketData);

        // 2. 受欢迎程度数据
        Map<String, Object> themePopularityData = formatThemePopularityData(themePopularity);
        result.put("themePopularityData", themePopularityData);

        // 3. 字数分布数据
        Map<String, Object> wordLengthData = formatThemeWordLengthData(wordLengthDistribution);
        result.put("wordLengthData", wordLengthData);

        return result;
    }

    /**
     * 格式化主题市场表现数据
     */
    private Map<String, Object> formatThemeMarketPerformanceData(List<Object[]> marketPerformance) {
        Map<String, Object> result = new HashMap<>();

        List<String> themeNames = new ArrayList<>();
        List<Number> popularityIndex = new ArrayList<>();
        List<Number> avgRead = new ArrayList<>();
        List<Number> avgFlower = new ArrayList<>();
        List<Number> avgHotScore = new ArrayList<>();
        List<Number> novelCount = new ArrayList<>();

        for (Object[] row : marketPerformance) {
            themeNames.add((String) row[0]);
            novelCount.add(((Number) row[1]).intValue());
            avgRead.add(Math.round(((Number) row[2]).doubleValue() * 10) / 10.0);
            avgFlower.add(Math.round(((Number) row[3]).doubleValue() * 10) / 10.0);
            avgHotScore.add(Math.round(((Number) row[4]).doubleValue() * 100) / 100.0);
            popularityIndex.add(Math.round(((Number) row[5]).doubleValue() * 100) / 100.0);
        }

        result.put("themeNames", themeNames);
        result.put("popularityIndex", popularityIndex);
        result.put("avgRead", avgRead);
        result.put("avgFlower", avgFlower);
        result.put("avgHotScore", avgHotScore);
        result.put("novelCount", novelCount);

        return result;
    }

    /**
     * 格式化主题受欢迎程度数据
     */
    private Map<String, Object> formatThemePopularityData(List<Object[]> themePopularity) {
        Map<String, Object> result = new HashMap<>();

        Map<String, List<Number>> maleThemeData = new HashMap<>();
        Map<String, List<Number>> femaleThemeData = new HashMap<>();
        Set<String> themeSet = new HashSet<>();

        // 初始化数据结构
        for (Object[] row : themePopularity) {
            String theme = (String) row[0];
            themeSet.add(theme);
            maleThemeData.put(theme, new ArrayList<>());
            femaleThemeData.put(theme, new ArrayList<>());
        }

        // 填充数据
        for (Object[] row : themePopularity) {
            String theme = (String) row[0];
            String channel = (String) row[1];
            Number novelCount = ((Number) row[2]).intValue();
            Number avgFlower = Math.round(((Number) row[3]).doubleValue() * 10) / 10.0;

            if ("男频".equals(channel)) {
                maleThemeData.get(theme).add(novelCount);
                maleThemeData.get(theme).add(avgFlower);
            } else {
                femaleThemeData.get(theme).add(novelCount);
                femaleThemeData.get(theme).add(avgFlower);
            }
        }

        // 构建数据列表
        List<String> sortedThemes = new ArrayList<>(themeSet);
        sortedThemes.sort(String::compareTo);

        List<List<Number>> maleData = new ArrayList<>();
        List<List<Number>> femaleData = new ArrayList<>();

        for (String theme : sortedThemes) {
            List<Number> maleStats = maleThemeData.get(theme);
            List<Number> femaleStats = femaleThemeData.get(theme);

            if (maleStats.size() == 2) {
                maleData.add(Arrays.asList(maleStats.get(0), maleStats.get(1)));
            } else {
                maleData.add(Arrays.asList(0, 0.0));
            }

            if (femaleStats.size() == 2) {
                femaleData.add(Arrays.asList(femaleStats.get(0), femaleStats.get(1)));
            } else {
                femaleData.add(Arrays.asList(0, 0.0));
            }
        }

        result.put("themes", sortedThemes);
        result.put("maleData", maleData);
        result.put("femaleData", femaleData);

        return result;
    }

    /**
     * 格式化主题字数分布数据
     */
    private Map<String, Object> formatThemeWordLengthData(List<Object[]> wordLengthDistribution) {
        Map<String, Object> result = new HashMap<>();

        List<String> lengthCategories = Arrays.asList("短篇(<10万)", "中篇(10-50万)", "长篇(50-100万)", "超长篇(>100万)");
        Set<String> themeSet = new HashSet<>();
        Map<String, Map<String, Integer>> themeLengthMap = new HashMap<>();

        // 收集所有主题和初始化数据结构
        for (Object[] row : wordLengthDistribution) {
            String theme = (String) row[0];
            themeSet.add(theme);
            themeLengthMap.put(theme, new HashMap<>());
            for (String category : lengthCategories) {
                themeLengthMap.get(theme).put(category, 0);
            }
        }

        // 填充数据
        for (Object[] row : wordLengthDistribution) {
            String theme = (String) row[0];
            String lengthCategory = (String) row[1];
            Integer count = ((Number) row[2]).intValue();
            themeLengthMap.get(theme).put(lengthCategory, count);
        }

        // 构建数据
        List<String> sortedThemes = new ArrayList<>(themeSet);
        sortedThemes.sort(String::compareTo);

        List<List<Integer>> lengthData = new ArrayList<>();
        for (String theme : sortedThemes) {
            List<Integer> themeData = new ArrayList<>();
            for (String category : lengthCategories) {
                themeData.add(themeLengthMap.get(theme).get(category));
            }
            lengthData.add(themeData);
        }

        result.put("themes", sortedThemes);
        result.put("lengthCategories", lengthCategories);
        result.put("lengthData", lengthData);

        return result;
    }

    // ==============================
    // 默认数据方法 - 频道对比
    // ==============================

    /**
     * 获取默认频道对比数据
     */
    private Map<String, Object> getDefaultChannelComparisonData() {
        Map<String, Object> result = new HashMap<>();

        // 默认主题分布数据
        Map<String, Object> channelThemeData = getDefaultChannelThemeDistributionData();
        result.put("channelThemeData", channelThemeData);

        // 默认收藏趋势数据
        Map<String, Object> flowerTrendData = getDefaultChannelFlowerTrendData();
        result.put("flowerTrendData", flowerTrendData);

        // 默认频道指标数据
        Map<String, Object> channelMetrics = getDefaultChannelMetricsData();
        result.put("channelMetrics", channelMetrics);

        return result;
    }

    private Map<String, Object> getDefaultChannelThemeDistributionData() {
        Map<String, Object> result = new HashMap<>();
        result.put("themes", Arrays.asList("玄幻", "仙侠", "都市", "历史", "科幻", "游戏", "悬疑", "言情"));
        result.put("maleData", Arrays.asList(1500, 1200, 800, 600, 400, 350, 300, 250));
        result.put("femaleData", Arrays.asList(800, 1000, 1500, 300, 200, 400, 200, 1800));
        return result;
    }

    private Map<String, Object> getDefaultChannelFlowerTrendData() {
        Map<String, Object> result = new HashMap<>();
        result.put("categories", Arrays.asList("男频", "女频"));
        result.put("flowerLevels", Arrays.asList("低收藏(<1000)", "中收藏(1000-5000)", "高收藏(5000-10000)", "超高收藏(>10000)"));
        result.put("maleData", Arrays.asList(1200, 600, 300, 100));
        result.put("femaleData", Arrays.asList(800, 700, 400, 200));
        return result;
    }

    private Map<String, Object> getDefaultChannelMetricsData() {
        Map<String, Object> result = new HashMap<>();
        result.put("indicators", Arrays.asList(
                "作品数量", "平均阅读量", "平均收藏量", "平均热度", "总阅读量", "最高阅读量", "最高收藏量", "最高热度"
        ));
        result.put("maleValues", Arrays.asList(
                2200L, 85000.5, 4500.3, 7.8, 187000000L, 9850000L, 98500L, 9.8
        ));
        result.put("femaleValues", Arrays.asList(
                1800L, 72000.8, 5200.7, 8.2, 129600000L, 8760000L, 87600L, 9.5
        ));
        return result;
    }

    private Map<String, Object> getDefaultChannelTopPerformanceData() {
        Map<String, Object> result = new HashMap<>();

        List<Map<String, Object>> maleTopList = new ArrayList<>();
        List<Map<String, Object>> femaleTopList = new ArrayList<>();

        String[] maleTitles = {"斗破苍穹", "凡人修仙传", "遮天", "仙逆", "武动乾坤", "大主宰", "星辰变", "神墓", "盘龙", "诛仙"};
        String[] femaleTitles = {"扶摇皇后", "凤囚凰", "庶女有毒", "知否知否", "琅琊榜", "花千骨", "三生三世", "楚乔传", "步步惊心", "甄嬛传"};

        for (int i = 0; i < 10; i++) {
            Map<String, Object> maleItem = new HashMap<>();
            maleItem.put("title", maleTitles[i]);
            maleItem.put("type", i % 2 == 0 ? "玄幻" : "仙侠");
            maleItem.put("allRead", 1000000 - i * 50000);
            maleItem.put("allFlower", 50000 - i * 2000);
            maleItem.put("hotScore", 9.5 - i * 0.1);
            maleItem.put("rank", i + 1);
            maleTopList.add(maleItem);

            Map<String, Object> femaleItem = new HashMap<>();
            femaleItem.put("title", femaleTitles[i]);
            femaleItem.put("type", i % 2 == 0 ? "言情" : "都市");
            femaleItem.put("allRead", 800000 - i * 40000);
            femaleItem.put("allFlower", 60000 - i * 2500);
            femaleItem.put("hotScore", 9.0 - i * 0.1);
            femaleItem.put("rank", i + 1);
            femaleTopList.add(femaleItem);
        }

        result.put("maleTopList", maleTopList);
        result.put("femaleTopList", femaleTopList);

        return result;
    }

    // ==============================
    // 默认数据方法 - 主题分析
    // ==============================

    /**
     * 获取默认主题分析数据
     */
    private Map<String, Object> getDefaultThemeAnalysisData() {
        Map<String, Object> result = new HashMap<>();

        // 默认市场表现数据
        Map<String, Object> themeMarketData = getDefaultThemeMarketPerformanceData();
        result.put("themeMarketData", themeMarketData);

        // 默认受欢迎程度数据
        Map<String, Object> themePopularityData = getDefaultThemePopularityData();
        result.put("themePopularityData", themePopularityData);

        // 默认字数分布数据
        Map<String, Object> wordLengthData = getDefaultThemeWordLengthData();
        result.put("wordLengthData", wordLengthData);

        return result;
    }

    private Map<String, Object> getDefaultThemeMarketPerformanceData() {
        Map<String, Object> result = new HashMap<>();
        result.put("themeNames", Arrays.asList("玄幻", "仙侠", "都市", "历史", "科幻", "游戏", "悬疑", "言情"));
        result.put("popularityIndex", Arrays.asList(85.5, 78.3, 72.8, 65.4, 58.9, 52.7, 48.3, 91.2));
        result.put("avgRead", Arrays.asList(85000.5, 78000.3, 72000.8, 65000.4, 58000.9, 52000.7, 48000.3, 91000.2));
        result.put("avgFlower", Arrays.asList(4500.3, 4200.7, 5200.8, 3500.5, 2800.9, 3200.4, 2800.2, 6800.6));
        result.put("avgHotScore", Arrays.asList(7.8, 7.5, 8.2, 6.9, 6.3, 6.1, 5.8, 8.8));
        result.put("novelCount", Arrays.asList(1500, 1200, 1800, 800, 600, 700, 500, 2000));
        return result;
    }

    private Map<String, Object> getDefaultThemePopularityData() {
        Map<String, Object> result = new HashMap<>();
        result.put("themes", Arrays.asList("玄幻", "仙侠", "都市", "历史", "科幻", "游戏", "悬疑", "言情"));

        List<List<Number>> maleData = new ArrayList<>();
        List<List<Number>> femaleData = new ArrayList<>();

        // 男频数据：[作品数量, 平均收藏量]
        maleData.add(Arrays.asList(1200, 4200.5));
        maleData.add(Arrays.asList(1000, 3800.3));
        maleData.add(Arrays.asList(600, 4800.8));
        maleData.add(Arrays.asList(500, 3200.7));
        maleData.add(Arrays.asList(400, 2600.4));
        maleData.add(Arrays.asList(350, 3000.2));
        maleData.add(Arrays.asList(300, 2600.1));
        maleData.add(Arrays.asList(200, 5800.9));

        // 女频数据
        femaleData.add(Arrays.asList(300, 4800.3));
        femaleData.add(Arrays.asList(200, 4600.5));
        femaleData.add(Arrays.asList(1200, 5600.7));
        femaleData.add(Arrays.asList(300, 3800.2));
        femaleData.add(Arrays.asList(200, 3000.8));
        femaleData.add(Arrays.asList(350, 3400.6));
        femaleData.add(Arrays.asList(200, 3000.3));
        femaleData.add(Arrays.asList(1800, 7800.5));

        result.put("maleData", maleData);
        result.put("femaleData", femaleData);

        return result;
    }

    private Map<String, Object> getDefaultThemeWordLengthData() {
        Map<String, Object> result = new HashMap<>();
        result.put("themes", Arrays.asList("玄幻", "仙侠", "都市", "历史", "科幻", "游戏", "悬疑", "言情"));
        result.put("lengthCategories", Arrays.asList("短篇(<10万)", "中篇(10-50万)", "长篇(50-100万)", "超长篇(>100万)"));

        List<List<Integer>> lengthData = new ArrayList<>();
        lengthData.add(Arrays.asList(50, 300, 800, 350));   // 玄幻
        lengthData.add(Arrays.asList(40, 250, 700, 210));   // 仙侠
        lengthData.add(Arrays.asList(150, 600, 800, 250));  // 都市
        lengthData.add(Arrays.asList(80, 400, 250, 70));    // 历史
        lengthData.add(Arrays.asList(100, 300, 150, 50));   // 科幻
        lengthData.add(Arrays.asList(120, 350, 180, 50));   // 游戏
        lengthData.add(Arrays.asList(90, 250, 120, 40));    // 悬疑
        lengthData.add(Arrays.asList(300, 800, 600, 300));  // 言情

        result.put("lengthData", lengthData);

        return result;
    }

    // ==============================
    // 作者分析图表数据方法 - 基于authorprofile表
    // ==============================

    /**
     * 获取作者综合分析图表数据
     */
    public Map<String, Object> getAuthorAnalysisChartData() {
        try {
            List<Object[]> levelDistribution = publicDataService.getAuthorLevelDistribution();
            List<Object[]> productivityAnalysis = publicDataService.getAuthorProductivityAnalysis();
            List<Object[]> readFlowerAnalysis = publicDataService.getAuthorReadFlowerAnalysis();
            List<Object[]> channelComparison = publicDataService.getChannelAuthorComparisonStats();
            List<Object[]> efficiencyAnalysis = publicDataService.getAuthorEfficiencyAnalysis();
            List<Object[]> authorDaysByLevel = publicDataService.getAuthorDaysByLevel();
            List<Object[]> topAuthorsByWorkCount = publicDataService.getTopAuthorsByMetric("workCount", 10);

            if (levelDistribution.isEmpty() || channelComparison.isEmpty()) {
                return getDefaultAuthorAnalysisData();
            }

            return formatAuthorAnalysisData(
                    levelDistribution,
                    productivityAnalysis,
                    readFlowerAnalysis,
                    efficiencyAnalysis,
                    authorDaysByLevel,
                    topAuthorsByWorkCount
            );
        } catch (Exception e) {
            return getDefaultAuthorAnalysisData();
        }
    }

    /**
     * 格式化作者综合分析数据
     */
    private Map<String, Object> formatAuthorAnalysisData(
            List<Object[]> levelDistribution,
            List<Object[]> productivityAnalysis,
            List<Object[]> readFlowerAnalysis,
            List<Object[]> efficiencyAnalysis,
            List<Object[]> authorDaysByLevel,
            List<Object[]> topAuthors) {

        Map<String, Object> result = new HashMap<>();

        // 1. 作者等级分布数据
        Map<String, Object> levelDistributionData = formatAuthorLevelDistributionData(levelDistribution);
        result.put("levelDistributionData", levelDistributionData);

        // 2. 作者等级创作天数分析数据
        Map<String, Object> channelComparisonData = formatAuthorDaysByLevelData(authorDaysByLevel);
        result.put("channelComparisonData", channelComparisonData);

        // 3. 作者创作效率分析数据
        Map<String, Object> efficiencyAnalysisData = formatAuthorEfficiencyData(efficiencyAnalysis);
        result.put("efficiencyAnalysisData", efficiencyAnalysisData);

        // 4. 作者生产力分析数据
        Map<String, Object> productivityData = formatAuthorProductivityData(productivityAnalysis);
        result.put("productivityData", productivityData);

        // 5. 作者阅读收藏分析数据
        Map<String, Object> readFlowerData = formatAuthorReadFlowerData(readFlowerAnalysis);
        result.put("readFlowerData", readFlowerData);

        // 6. TOP作者排行榜数据
        Map<String, Object> topAuthorsData = formatTopAuthorsData(topAuthors);
        result.put("topAuthorsData", topAuthorsData);

        return result;
    }

    /**
     * 格式化作者等级分布数据
     */
    private Map<String, Object> formatAuthorLevelDistributionData(List<Object[]> levelDistribution) {
        Map<String, Object> result = new HashMap<>();

        // 收集所有等级
        Set<String> levelSet = new HashSet<>();
        Map<String, Map<String, Integer>> channelLevelMap = new HashMap<>();
        channelLevelMap.put("男频", new HashMap<>());
        channelLevelMap.put("女频", new HashMap<>());

        for (Object[] row : levelDistribution) {
            String channel = (String) row[0];
            String level = (String) row[1];
            Integer count = ((Number) row[2]).intValue();

            levelSet.add(level);
            channelLevelMap.get(channel).put(level, count);
        }

        // 按等级排序
        List<String> sortedLevels = new ArrayList<>(levelSet);
        sortedLevels.sort(String::compareTo);

        // 构建数据数组
        List<Integer> maleData = new ArrayList<>();
        List<Integer> femaleData = new ArrayList<>();

        for (String level : sortedLevels) {
            maleData.add(channelLevelMap.get("男频").getOrDefault(level, 0));
            femaleData.add(channelLevelMap.get("女频").getOrDefault(level, 0));
        }

        result.put("levels", sortedLevels);
        result.put("maleData", maleData);
        result.put("femaleData", femaleData);

        return result;
    }

    /**
     * 格式化频道作者对比数据
     */
    private Map<String, Object> formatChannelAuthorComparisonData(List<Object[]> channelComparison) {
        Map<String, Object> result = new HashMap<>();

        List<String> indicators = Arrays.asList(
                "作者数量", "平均作品数", "平均阅读量", "平均收藏量", "平均总字数", "平均创作天数",
                "最高作品数", "最高阅读量", "最高收藏量"
        );

        List<Number> maleValues = new ArrayList<>();
        List<Number> femaleValues = new ArrayList<>();

        Map<String, Object[]> statsByChannel = new HashMap<>();
        for (Object[] row : channelComparison) {
            String channel = (String) row[0];
            statsByChannel.put(channel, row);
        }

        // 男频数据
        Object[] maleStats = statsByChannel.get("男频");
        Object[] femaleStats = statsByChannel.get("女频");

        if (maleStats != null) {
            maleValues.add(((Number) maleStats[1]).longValue());  // 作者数量
            maleValues.add(Math.round(((Number) maleStats[2]).doubleValue() * 10) / 10.0);  // 平均作品数
            maleValues.add(Math.round(((Number) maleStats[3]).doubleValue() * 10) / 10.0);  // 平均阅读量
            maleValues.add(Math.round(((Number) maleStats[4]).doubleValue() * 10) / 10.0);  // 平均收藏量
            maleValues.add(Math.round(((Number) maleStats[5]).doubleValue() * 10) / 10.0);  // 平均总字数
            maleValues.add(Math.round(((Number) maleStats[6]).doubleValue() * 10) / 10.0);  // 平均创作天数
            maleValues.add(((Number) maleStats[7]).longValue());  // 最高作品数
            maleValues.add(((Number) maleStats[8]).longValue());  // 最高阅读量
            maleValues.add(((Number) maleStats[9]).longValue());  // 最高收藏量
        } else {
            for (int i = 0; i < 9; i++) maleValues.add(0);
        }

        if (femaleStats != null) {
            femaleValues.add(((Number) femaleStats[1]).longValue());
            femaleValues.add(Math.round(((Number) femaleStats[2]).doubleValue() * 10) / 10.0);
            femaleValues.add(Math.round(((Number) femaleStats[3]).doubleValue() * 10) / 10.0);
            femaleValues.add(Math.round(((Number) femaleStats[4]).doubleValue() * 10) / 10.0);
            femaleValues.add(Math.round(((Number) femaleStats[5]).doubleValue() * 10) / 10.0);
            femaleValues.add(Math.round(((Number) femaleStats[6]).doubleValue() * 10) / 10.0);
            femaleValues.add(((Number) femaleStats[7]).longValue());
            femaleValues.add(((Number) femaleStats[8]).longValue());
            femaleValues.add(((Number) femaleStats[9]).longValue());
        } else {
            for (int i = 0; i < 9; i++) femaleValues.add(0);
        }

        result.put("indicators", indicators);
        result.put("maleValues", maleValues);
        result.put("femaleValues", femaleValues);

        return result;
    }

    /**
     * 格式化作者等级创作天数分析数据
     */
    private Map<String, Object> formatAuthorDaysByLevelData(List<Object[]> authorDaysByLevel) {
        Map<String, Object> result = new HashMap<>();

        // 收集所有等级
        Set<String> levelSet = new HashSet<>();
        Map<String, Map<String, Double>> channelDaysMap = new HashMap<>();
        channelDaysMap.put("男频", new HashMap<>());
        channelDaysMap.put("女频", new HashMap<>());

        for (Object[] row : authorDaysByLevel) {
            String channel = (String) row[0];
            String level = (String) row[1];
            Double avgAuthorDays = ((Number) row[3]).doubleValue(); // avg_author_days

            levelSet.add(level);
            channelDaysMap.get(channel).put(level, avgAuthorDays);
        }

        // 按等级排序
        List<String> sortedLevels = new ArrayList<>(levelSet);
        sortedLevels.sort(String::compareTo);

        // 构建数据数组
        List<Double> maleValues = new ArrayList<>();
        List<Double> femaleValues = new ArrayList<>();

        for (String level : sortedLevels) {
            maleValues.add(channelDaysMap.get("男频").getOrDefault(level, 0.0));
            femaleValues.add(channelDaysMap.get("女频").getOrDefault(level, 0.0));
        }

        result.put("indicators", sortedLevels);
        result.put("maleValues", maleValues);
        result.put("femaleValues", femaleValues);

        return result;
    }

    /**
     * 格式化作者类型分布数据（用于替换原频道作者对比）
     */
    private Map<String, Object> formatAuthorTypeDistributionData(List<Object[]> efficiencyAnalysis) {
        // 使用作者创作效率数据，但重命名字段以匹配前端期望
        Map<String, Object> efficiencyData = formatAuthorEfficiencyData(efficiencyAnalysis);

        Map<String, Object> result = new HashMap<>();
        result.put("indicators", efficiencyData.get("experienceLevels"));
        result.put("maleValues", efficiencyData.get("maleData"));
        result.put("femaleValues", efficiencyData.get("femaleData"));

        return result;
    }

    /**
     * 格式化作者创作效率数据
     */
    private Map<String, Object> formatAuthorEfficiencyData(List<Object[]> efficiencyAnalysis) {
        Map<String, Object> result = new HashMap<>();

        List<String> experienceLevels = Arrays.asList("新手作者", "成长作者", "成熟作者", "高产作者");
        Map<String, Map<String, Integer>> channelExperienceMap = new HashMap<>();
        channelExperienceMap.put("男频", new HashMap<>());
        channelExperienceMap.put("女频", new HashMap<>());

        // 初始化
        for (String level : experienceLevels) {
            channelExperienceMap.get("男频").put(level, 0);
            channelExperienceMap.get("女频").put(level, 0);
        }

        // 填充数据
        for (Object[] row : efficiencyAnalysis) {
            String channel = (String) row[0];
            String experienceLevel = (String) row[1];
            Integer count = ((Number) row[2]).intValue();

            channelExperienceMap.get(channel).put(experienceLevel, count);
        }

        // 构建数据数组
        List<Integer> maleData = new ArrayList<>();
        List<Integer> femaleData = new ArrayList<>();

        for (String level : experienceLevels) {
            maleData.add(channelExperienceMap.get("男频").get(level));
            femaleData.add(channelExperienceMap.get("女频").get(level));
        }

        result.put("experienceLevels", experienceLevels);
        result.put("maleData", maleData);
        result.put("femaleData", femaleData);

        return result;
    }

    /**
     * 格式化作者生产力数据
     */
    private Map<String, Object> formatAuthorProductivityData(List<Object[]> productivityAnalysis) {
        Map<String, Object> result = new HashMap<>();

        List<Map<String, Object>> authorData = new ArrayList<>();
        for (Object[] row : productivityAnalysis) {
            String author = (String) row[0];
            Integer workCount = ((Number) row[1]).intValue();
            Long authorWords = ((Number) row[2]).longValue();
            Integer authorDays = ((Number) row[3]).intValue();
            Long wordsPerDay = ((Number) row[4]).longValue();
            Long avgWordsPerWork = ((Number) row[5]).longValue();

            Map<String, Object> item = new HashMap<>();
            item.put("author", author);
            item.put("workCount", workCount);
            item.put("authorWords", authorWords);
            item.put("authorDays", authorDays);
            item.put("wordsPerDay", wordsPerDay);
            item.put("avgWordsPerWork", avgWordsPerWork);

            authorData.add(item);
        }

        result.put("authorData", authorData);
        return result;
    }

    /**
     * 格式化作者阅读收藏数据
     */
    private Map<String, Object> formatAuthorReadFlowerData(List<Object[]> readFlowerAnalysis) {
        Map<String, Object> result = new HashMap<>();

        List<Map<String, Object>> authorPoints = new ArrayList<>();
        for (Object[] row : readFlowerAnalysis) {
            String author = (String) row[0];
            Long totalRead = ((Number) row[1]).longValue();
            Long totalFlower = ((Number) row[2]).longValue();
            Double flowerRatio = ((Number) row[3]).doubleValue();
            String channel = (String) row[4];
            int workCount = ((Number) row[5]).intValue();

            Map<String, Object> point = new HashMap<>();
            point.put("author", author);
            point.put("totalRead", totalRead);
            point.put("totalFlower", totalFlower);
            point.put("flowerRatio", flowerRatio);
            point.put("channel", channel);
            point.put("workCount", workCount);
            point.put("symbolSize", Math.min(30, Math.max(5, workCount / 2))); // 符号大小基于作品数量

            authorPoints.add(point);
        }

        result.put("authorPoints", authorPoints);
        return result;
    }

    /**
     * 格式化TOP作者数据
     */
    private Map<String, Object> formatTopAuthorsData(List<Object[]> topAuthors) {
        Map<String, Object> result = new HashMap<>();

        List<Map<String, Object>> authorList = new ArrayList<>();
        for (Object[] row : topAuthors) {
            String author = (String) row[0];
            Integer metricValue = ((Number) row[1]).intValue();
            String channel = (String) row[2];
            Long totalRead = ((Number) row[4]).longValue();
            Long totalFlower = ((Number) row[5]).longValue();

            Map<String, Object> item = new HashMap<>();
            item.put("author", author);
            item.put("workCount", metricValue); // 按作品数量排序的指标值
            item.put("channel", channel);
            item.put("totalRead", totalRead);
            item.put("totalFlower", totalFlower);
            item.put("rank", authorList.size() + 1);

            authorList.add(item);
        }

        result.put("topAuthors", authorList);
        return result;
    }

    /**
     * 获取默认作者分析数据
     */
    private Map<String, Object> getDefaultAuthorAnalysisData() {
        Map<String, Object> result = new HashMap<>();

        // 默认等级分布数据
        Map<String, Object> levelDistributionData = getDefaultAuthorLevelDistributionData();
        result.put("levelDistributionData", levelDistributionData);

        // 默认频道对比数据
        Map<String, Object> channelComparisonData = getDefaultChannelAuthorComparisonData();
        result.put("channelComparisonData", channelComparisonData);

        // 默认效率分析数据
        Map<String, Object> efficiencyAnalysisData = getDefaultAuthorEfficiencyData();
        result.put("efficiencyAnalysisData", efficiencyAnalysisData);

        // 默认生产力数据
        Map<String, Object> productivityData = getDefaultAuthorProductivityData();
        result.put("productivityData", productivityData);

        // 默认阅读收藏数据
        Map<String, Object> readFlowerData = getDefaultAuthorReadFlowerData();
        result.put("readFlowerData", readFlowerData);

        // 默认TOP作者数据
        Map<String, Object> topAuthorsData = getDefaultTopAuthorsData();
        result.put("topAuthorsData", topAuthorsData);

        return result;
    }

    private Map<String, Object> getDefaultAuthorLevelDistributionData() {
        Map<String, Object> result = new HashMap<>();
        result.put("levels", Arrays.asList("Lv1", "Lv2", "Lv3", "Lv4", "Lv5", "Lv6"));
        result.put("maleData", Arrays.asList(120, 85, 60, 40, 25, 15));
        result.put("femaleData", Arrays.asList(95, 70, 50, 35, 20, 10));
        return result;
    }

    private Map<String, Object> getDefaultChannelAuthorComparisonData() {
        Map<String, Object> result = new HashMap<>();
        result.put("indicators", Arrays.asList(
                "作者数量", "平均作品数", "平均阅读量", "平均收藏量", "平均总字数", "平均创作天数",
                "最高作品数", "最高阅读量", "最高收藏量"
        ));
        result.put("maleValues", Arrays.asList(
                345L, 5.8, 2850000.5, 14500.3, 850000.7, 450.2, 42L, 98500000L, 985000L
        ));
        result.put("femaleValues", Arrays.asList(
                280L, 6.2, 3200000.8, 16800.7, 920000.3, 480.5, 38L, 87600000L, 876000L
        ));
        return result;
    }

    private Map<String, Object> getDefaultAuthorEfficiencyData() {
        Map<String, Object> result = new HashMap<>();
        result.put("experienceLevels", Arrays.asList("新手作者", "成长作者", "成熟作者", "高产作者"));
        result.put("maleData", Arrays.asList(120, 85, 60, 25));
        result.put("femaleData", Arrays.asList(95, 70, 50, 20));
        return result;
    }

    private Map<String, Object> getDefaultAuthorProductivityData() {
        Map<String, Object> result = new HashMap<>();
        List<Map<String, Object>> authorData = new ArrayList<>();
        String[] authors = {"作者A", "作者B", "作者C", "作者D", "作者E", "作者F", "作者G", "作者H", "作者I", "作者J"};
        for (int i = 0; i < 10; i++) {
            Map<String, Object> item = new HashMap<>();
            item.put("author", authors[i]);
            item.put("workCount", 10 - i);
            item.put("authorWords", (10 - i) * 100000L);
            item.put("authorDays", (10 - i) * 100);
            item.put("wordsPerDay", 1000L);
            item.put("avgWordsPerWork", 100000L);
            authorData.add(item);
        }
        result.put("authorData", authorData);
        return result;
    }

    private Map<String, Object> getDefaultAuthorReadFlowerData() {
        Map<String, Object> result = new HashMap<>();
        List<Map<String, Object>> authorPoints = new ArrayList<>();
        String[] authors = {"作者A", "作者B", "作者C", "作者D", "作者E", "作者F", "作者G", "作者H", "作者I", "作者J"};
        for (int i = 0; i < 10; i++) {
            Map<String, Object> point = new HashMap<>();
            point.put("author", authors[i]);
            point.put("totalRead", 10000000L - i * 500000L);
            point.put("totalFlower", 50000L - i * 2000L);
            point.put("flowerRatio", 5.0 + i * 0.1);
            point.put("channel", i % 2 == 0 ? "男频" : "女频");
            point.put("workCount", 10 - i);
            point.put("symbolSize", 15 - i);
            authorPoints.add(point);
        }
        result.put("authorPoints", authorPoints);
        return result;
    }

    private Map<String, Object> getDefaultTopAuthorsData() {
        Map<String, Object> result = new HashMap<>();
        List<Map<String, Object>> authorList = new ArrayList<>();
        String[] authors = {"作者A", "作者B", "作者C", "作者D", "作者E", "作者F", "作者G", "作者H", "作者I", "作者J"};
        for (int i = 0; i < 10; i++) {
            Map<String, Object> item = new HashMap<>();
            item.put("author", authors[i]);
            item.put("workCount", 10 - i);
            item.put("channel", i % 2 == 0 ? "男频" : "女频");
            item.put("totalRead", 10000000L - i * 500000L);
            item.put("totalFlower", 50000L - i * 2000L);
            item.put("rank", i + 1);
            authorList.add(item);
        }
        result.put("topAuthors", authorList);
        return result;
    }
}