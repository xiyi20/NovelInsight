package com.novelinsight.controller;

import com.novelinsight.model.User;
import com.novelinsight.service.ChartDataService;
import com.novelinsight.service.UserService;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/app")
public class HomeController {

    private final UserService userService;
    private final ChartDataService chartDataService;

    public HomeController(UserService userService, ChartDataService chartDataService) {
        this.userService = userService;
        this.chartDataService = chartDataService;
    }

    @GetMapping
    public String index(Model model, @AuthenticationPrincipal UserDetails userDetails) {
        String username = userDetails.getUsername();
        User user = userService.findByUsername(username);

        // 获取首页数据
        var indexData = chartDataService.getIndexData();
        var lineChartData = chartDataService.getIndexLineChart();
        var funnelData = chartDataService.getIndexFunnelChart();
        var pieChartData = chartDataService.getIndexPieChart();
        var barLineData = chartDataService.getBarLineData();

        model.addAttribute("userInfo", user);
        model.addAttribute("maxNovel", indexData.get("maxNovel"));
        model.addAttribute("maxReadNum", indexData.get("maxReadNum"));
        model.addAttribute("maxFlowNum", indexData.get("maxFlowNum"));
        model.addAttribute("maxRewardNum", indexData.get("maxRewardNum"));
        model.addAttribute("maxAuthor", indexData.get("maxAuthor"));
        model.addAttribute("totalNovels", indexData.get("totalNovels"));
        model.addAttribute("avgReadNum", indexData.get("avgReadNum"));
        model.addAttribute("avgFlowNum", indexData.get("avgFlowNum"));
        model.addAttribute("totalReward", indexData.get("totalReward"));
        model.addAttribute("totalWords", indexData.get("totalWords"));
        model.addAttribute("topType", indexData.get("topType"));
        model.addAttribute("topTypeCount", indexData.get("topTypeCount"));

        model.addAttribute("xLineData1", lineChartData.get("xLineData1"));
        model.addAttribute("yLineData1", lineChartData.get("yLineData1"));
        model.addAttribute("yLineData2", lineChartData.get("yLineData2"));

        model.addAttribute("FunnelData", funnelData);
        model.addAttribute("PieData", pieChartData.get("PieData"));
        model.addAttribute("pieXData", pieChartData.get("pieXData"));

        model.addAttribute("xData1", barLineData.get("xData1"));
        model.addAttribute("yData1", barLineData.get("yData1"));
        model.addAttribute("yData2", barLineData.get("yData2"));
        model.addAttribute("currentPage", "index");

        return "index";
    }
}