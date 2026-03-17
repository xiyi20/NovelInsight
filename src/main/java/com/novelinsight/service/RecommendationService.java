package com.novelinsight.service;

import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 推荐系统服务，对应Python中的machine/index.py
 * 实现协同过滤推荐算法
 */
@Service
public class RecommendationService {

    private final MySqlQueryService mySqlQueryService;
    private final HiveService hiveService;

    public RecommendationService(MySqlQueryService mySqlQueryService, HiveService hiveService) {
        this.mySqlQueryService = mySqlQueryService;
        this.hiveService = hiveService;
    }

    /**
     * 获取用户评分数据（对应getUser_ratings函数）
     * 返回Map<用户名, Map<小说ID, 点击次数>>
     */
    public Map<String, Map<Integer, Integer>> getUserRatings() {
        Map<String, Map<Integer, Integer>> userRatings = new HashMap<>();

        // 查询所有用户 - 使用queryForList获取Map以便通过列名访问
        List<Map<String, Object>> userList = mySqlQueryService.queryForList("SELECT * FROM user");
        // 查询所有历史记录
        List<Map<String, Object>> historyList = mySqlQueryService.queryForList("SELECT * FROM history");

        for (Map<String, Object> user : userList) {
            Long userId;
            String userName;

            // 安全获取userId
            Object idObj = user.get("id");
            if (idObj instanceof Number) {
                userId = ((Number) idObj).longValue();
            } else {
                continue; // 跳过无效用户
            }

            // 安全获取userName，处理可能的Date类型
            Object usernameObj = user.get("username");
            if (usernameObj instanceof String) {
                userName = (String) usernameObj;
            } else {
                // 如果不是String，尝试转换为String
                userName = String.valueOf(usernameObj);
            }

            for (Map<String, Object> history : historyList) {
                // 安全获取novelId
                Object novelIdObj = history.get("novelId");
                String novelId;
                if (novelIdObj instanceof String) {
                    novelId = (String) novelIdObj;
                } else if (novelIdObj != null) {
                    novelId = String.valueOf(novelIdObj);
                } else {
                    continue; // novelId为空，跳过
                }

                // 安全获取historyUserId
                Object userIdObj = history.get("user_id");
                long historyUserId;
                if (userIdObj instanceof Number) {
                    historyUserId = ((Number) userIdObj).longValue();
                } else {
                    continue; // 无效的user_id
                }

                if (!userId.equals(historyUserId)) {
                    continue;
                }

                try {
                    // 检查历史记录是否存在
                    List<Object[]> existHistory = mySqlQueryService.query(
                        "SELECT id FROM history WHERE novelId = ? AND user_id = ?",
                        novelId, userId
                    );

                    if (!existHistory.isEmpty()) {
                        // 查询Hive中是否存在该小说
                        List<Object[]> novelData = hiveService.query(
                            "SELECT id FROM noveldata WHERE id = ?",
                            Integer.parseInt(novelId)
                        );

                        if (!novelData.isEmpty()) {
                            int novelIdInt = Integer.parseInt(novelId);
                            // 安全获取historyCount
                            Object countObj = history.get("history_count");
                            int historyCount;
                            if (countObj instanceof Number) {
                                historyCount = ((Number) countObj).intValue();
                            } else if (countObj != null) {
                                try {
                                    historyCount = Integer.parseInt(String.valueOf(countObj));
                                } catch (NumberFormatException e) {
                                    historyCount = 1; // 默认值
                                }
                            } else {
                                historyCount = 1; // 默认值
                            }

                            userRatings.computeIfAbsent(userName, k -> new HashMap<>())
                                    .put(novelIdInt, historyCount);
                        }
                    }
                } catch (NumberFormatException e) {
                    // 如果novelId不是数字，跳过
                }
            }
        }

        return userRatings;
    }

    /**
     * 基于物品的协同过滤推荐（对应item_col_filter函数）
     * @param userName 目标用户名
     * @param userRatings 用户评分数据
     * @param topN 每个物品考虑的相似物品数量，默认3
     * @return 推荐的小说ID列表
     */
    public List<Integer> itemCollaborativeFilter(String userName,
                                                 Map<String, Map<Integer, Integer>> userRatings,
                                                 int topN) {
        Map<Integer, Integer> targetUserRatings = userRatings.get(userName);
        if (targetUserRatings == null || targetUserRatings.isEmpty()) {
            return Collections.emptyList();
        }

        // 获取所有物品和用户列表
        Set<Integer> allItems = new HashSet<>();
        List<String> allUsers = new ArrayList<>(userRatings.keySet());

        // 如果用户数量少于2，无法进行协同过滤
        if (allUsers.size() < 2) {
            return Collections.emptyList();
        }

        for (Map<Integer, Integer> ratings : userRatings.values()) {
            allItems.addAll(ratings.keySet());
        }

        // 构建物品-用户评分矩阵：物品 -> 用户 -> 评分
        Map<Integer, Map<String, Integer>> itemUserMatrix = new HashMap<>();
        for (String user : allUsers) {
            Map<Integer, Integer> ratings = userRatings.get(user);
            if (ratings != null) {
                for (Map.Entry<Integer, Integer> entry : ratings.entrySet()) {
                    int itemId = entry.getKey();
                    int rating = entry.getValue();
                    itemUserMatrix.computeIfAbsent(itemId, k -> new HashMap<>())
                            .put(user, rating);
                }
            }
        }

        // 为目标用户评分过的每个物品计算相似物品
        Map<Integer, Double> itemScores = new HashMap<>(); // 物品推荐得分

        for (Map.Entry<Integer, Integer> entry : targetUserRatings.entrySet()) {
            int ratedItem = entry.getKey();
            int userRating = entry.getValue(); // 用户对该物品的评分

            // 计算该物品与其他物品的相似度
            Map<Integer, Double> similarities = new HashMap<>();

            for (int otherItem : allItems) {
                if (otherItem == ratedItem) {
                    continue;
                }

                // 计算两个物品的相似度（余弦相似度）
                double similarity = calculateItemSimilarity(ratedItem, otherItem,
                                                           itemUserMatrix, allUsers);
                similarities.put(otherItem, similarity);
            }

            // 选择最相似的topN个物品
            List<Map.Entry<Integer, Double>> sortedSimilarities = similarities.entrySet()
                    .stream()
                    .sorted(Map.Entry.<Integer, Double>comparingByValue().reversed())
                    .limit(topN)
                    .toList();

            // 为每个相似物品计算推荐得分：相似度 × 用户评分
            for (Map.Entry<Integer, Double> simEntry : sortedSimilarities) {
                int similarItem = simEntry.getKey();
                double similarity = simEntry.getValue();
                double score = similarity * userRating;

                itemScores.put(similarItem,
                        itemScores.getOrDefault(similarItem, 0.0) + score);
            }
        }

        // 按推荐得分排序，排除用户已评分的物品
        return itemScores.entrySet()
                .stream()
                .filter(entry -> !targetUserRatings.containsKey(entry.getKey()))
                .sorted(Map.Entry.<Integer, Double>comparingByValue().reversed())
                .map(Map.Entry::getKey)
                .toList();
    }

    /**
     * 计算两个物品之间的相似度（余弦相似度）
     */
    private double calculateItemSimilarity(int item1, int item2,
                                          Map<Integer, Map<String, Integer>> itemUserMatrix,
                                          List<String> allUsers) {
        Map<String, Integer> item1Ratings = itemUserMatrix.getOrDefault(item1, new HashMap<>());
        Map<String, Integer> item2Ratings = itemUserMatrix.getOrDefault(item2, new HashMap<>());

        // 构建评分向量（按用户顺序）
        double[] vector1 = new double[allUsers.size()];
        double[] vector2 = new double[allUsers.size()];

        for (int i = 0; i < allUsers.size(); i++) {
            String user = allUsers.get(i);
            vector1[i] = item1Ratings.getOrDefault(user, 0);
            vector2[i] = item2Ratings.getOrDefault(user, 0);
        }

        return cosineSimilarity(vector1, vector2);
    }


    /**
     * 计算余弦相似度
     */
    private double cosineSimilarity(double[] vectorA, double[] vectorB) {
        if (vectorA.length != vectorB.length) {
            throw new IllegalArgumentException("向量长度必须相同");
        }

        double dotProduct = 0.0;
        double normA = 0.0;
        double normB = 0.0;

        for (int i = 0; i < vectorA.length; i++) {
            dotProduct += vectorA[i] * vectorB[i];
            normA += Math.pow(vectorA[i], 2);
            normB += Math.pow(vectorB[i], 2);
        }

        if (normA == 0 || normB == 0) {
            return 0.0;
        }

        return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
    }

    /**
     * 获取推荐小说详情
     */
    public List<Object[]> getRecommendationDetails(List<Integer> novelIds) {
        if (novelIds.isEmpty()) {
            return Collections.emptyList();
        }

        // 构建IN查询
        String ids = novelIds.stream()
                .map(String::valueOf)
                .collect(Collectors.joining(","));
        String sql = "SELECT * FROM noveldata WHERE id IN (" + ids + ")";

        return hiveService.query(sql);
    }

    /**
     * 获取热门推荐（备选方案，当协同过滤没有结果时使用）
     */
    public List<Object[]> getPopularRecommendations(int limit) {
        String sql = "SELECT * FROM noveldata ORDER BY allRead DESC LIMIT " + limit;
        return hiveService.query(sql);
    }
}