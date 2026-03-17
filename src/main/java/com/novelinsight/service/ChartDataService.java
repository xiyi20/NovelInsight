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
     * 添加历史记录（对应addHistoryData函数）
     */
    // 此功能由HistoryService处理

    /**
     * 获取用户历史记录（对应getUserHistory函数）
     */
    // 此功能由HistoryService处理

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
        for (Object[] row : authorMaxWordList) {
            Map<String, Object> item = new HashMap<>();
            item.put("name", row[0] + ":" + row[1]); // 作者:小说名
            item.put("value", ((Number) row[2]).longValue()); // 字数
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
}