package com.novelinsight.controller;

import com.novelinsight.model.User;
import com.novelinsight.service.HistoryService;
import com.novelinsight.service.RecommendationService;
import com.novelinsight.service.UserService;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.*;
import java.util.stream.Collectors;

@Controller
@RequestMapping("/app")
public class RecommendController {

    private final UserService userService;
    private final RecommendationService recommendationService;
    private final HistoryService historyService;

    public RecommendController(UserService userService, RecommendationService recommendationService, HistoryService historyService) {
        this.userService = userService;
        this.recommendationService = recommendationService;
        this.historyService = historyService;
    }

    /**
     * 推荐页面（对应recommend视图）
     */
    @GetMapping("/recommend")
    public String recommend(@AuthenticationPrincipal UserDetails userDetails, Model model) {
        String username = userDetails.getUsername();
        User user = userService.findByUsername(username);

        List<Object[]> recommendData = new ArrayList<>();

        // 获取用户已收藏的小说ID集合（从历史记录中）
        Set<Integer> userCollectedNovelIds = getUserCollectedNovelIds(user);
        try {
            // 获取用户评分数据
            Map<String, Map<Integer, Integer>> userRatings = recommendationService.getUserRatings();

            // 如果当前用户有评分数据，进行协同过滤推荐
            if (userRatings.containsKey(username)) {
                List<Integer> recommendItems = recommendationService.itemCollaborativeFilter(username, userRatings, 5);

                if (!recommendItems.isEmpty()) {
                    // 过滤掉用户已收藏的小说
                    recommendItems = recommendItems.stream().filter(itemId -> !userCollectedNovelIds.contains(itemId)).toList();

                    // 获取推荐小说详情
                    List<Object[]> details = recommendationService.getRecommendationDetails(recommendItems);
                    recommendData.addAll(details);
                }
            }

            // 如果推荐结果为空或不足，补充热门推荐
            if (recommendData.isEmpty()) {
                // 获取热门推荐并过滤用户已收藏的小说
                List<Object[]> popularRecommendations = recommendationService.getPopularRecommendations(20);
                recommendData = filterCollectedNovels(popularRecommendations, userCollectedNovelIds, 10);
            }

            // 如果推荐结果超过6个，随机选择6个
            if (recommendData.size() >= 6) {
                Collections.shuffle(recommendData);
                recommendData = recommendData.subList(0, 6);
            }

        } catch (Exception e) {
            // 发生异常时返回热门推荐
            e.printStackTrace();
            // 发生异常时也需要过滤已收藏的小说
            List<Object[]> popularRecommendations = recommendationService.getPopularRecommendations(10);
            recommendData = filterCollectedNovels(popularRecommendations, userCollectedNovelIds, 6);
        }

        model.addAttribute("userInfo", user);
        model.addAttribute("recommendData", recommendData);
        model.addAttribute("currentPage", "recommend");

        return "recommend";
    }

    /**
     * 获取用户已收藏的小说ID集合
     */
    private Set<Integer> getUserCollectedNovelIds(User user) {
        // 获取用户的历史记录（收藏）
        List<Object[]> userHistory = historyService.getUserHistory(user);
        return userHistory.stream()
                .filter(Objects::nonNull)
                .map(arr -> ((Number) arr[21]).intValue())
                .collect(Collectors.toSet());
    }

    /**
     * 过滤掉用户已收藏的小说，并限制返回数量
     */
    private List<Object[]> filterCollectedNovels(List<Object[]> novels, Set<Integer> collectedNovelIds, int limit) {
        List<Object[]> filtered = new ArrayList<>();
        for (Object[] novel : novels) {
            if (novel != null && novel.length > 0) {
                int novelId = ((Number) novel[21]).intValue();
                if (!collectedNovelIds.contains(novelId)) {
                    filtered.add(novel);
                    if (filtered.size() >= limit) {
                        break;
                    }
                }
            }
        }
        return filtered;
    }
}