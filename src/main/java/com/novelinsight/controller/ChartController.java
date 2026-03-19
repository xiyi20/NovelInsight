package com.novelinsight.controller;

import com.novelinsight.model.User;
import com.novelinsight.service.ChartDataService;
import com.novelinsight.service.PublicDataService;
import com.novelinsight.service.UserService;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Controller
@RequestMapping("/app")
public class ChartController {

    private final UserService userService;
    private final ChartDataService chartDataService;
    private final PublicDataService publicDataService;

    public ChartController(UserService userService,
                          ChartDataService chartDataService,
                          PublicDataService publicDataService) {
        this.userService = userService;
        this.chartDataService = chartDataService;
        this.publicDataService = publicDataService;
    }

    /**
     * 类型图表页面（对应typeChart视图）
     * 现在合并了主题分析功能
     */
    @GetMapping("/typeChart")
    public String typeChart(@AuthenticationPrincipal UserDetails userDetails, Model model) {
        String username = userDetails.getUsername();
        User user = userService.findByUsername(username);

        // 获取类型图表数据
        var chartData = chartDataService.getTypeChartData();

        // 获取主题分析数据
        var themeChartData = chartDataService.getThemeAnalysisChartData();

        model.addAttribute("userInfo", user);
        model.addAttribute("xLine1", chartData.get("xLine1"));
        model.addAttribute("yLine1", chartData.get("yLine1"));
        model.addAttribute("yLine2", chartData.get("yLine2"));
        model.addAttribute("xBar1", chartData.get("xBar1"));
        model.addAttribute("yBar1", chartData.get("yBar1"));
        model.addAttribute("pieData1", chartData.get("pieData1"));

        // 添加主题分析数据
        model.addAttribute("marketData", themeChartData.get("themeMarketData"));
        model.addAttribute("popularityData", themeChartData.get("themePopularityData"));
        model.addAttribute("wordLengthData", themeChartData.get("wordLengthData"));

        model.addAttribute("currentPage", "typeChart");

        return "typeChart";
    }

    /**
     * 小说信息图表页面（对应infoChart视图）
     */
    @GetMapping("/infoChart")
    public String infoChart(@AuthenticationPrincipal UserDetails userDetails,
                           @RequestParam(required = false) String typeName,
                            Model model) {
        String username = userDetails.getUsername();
        User user = userService.findByUsername(username);

        // 获取所有小说类型
        List<Object[]> novelDataList = publicDataService.getNovelData();
        Set<String> typeSet = novelDataList.stream()
                .map(row -> (String) row[0])
                .collect(Collectors.toSet());
        List<String> typeList = typeSet.stream().sorted().collect(Collectors.toList());

        // 默认类型
        String defaultType = typeName != null ? typeName : (typeList.isEmpty() ? "" : typeList.get(0));

        // 获取图表数据
        var chartData = chartDataService.getNovelInfo(defaultType);

        model.addAttribute("userInfo", user);
        model.addAttribute("typeList", typeList);
        model.addAttribute("defaultType", defaultType);
        model.addAttribute("xData1", chartData.get("xData1"));
        model.addAttribute("yData1", chartData.get("yData1"));
        model.addAttribute("xData2", chartData.get("xData2"));
        model.addAttribute("yData2", chartData.get("yData2"));
        model.addAttribute("currentPage", "infoChart");

        return "infoChart";
    }

    /**
     * 时间图表页面（对应timeChart视图）
     */
    @GetMapping("/timeChart")
    public String timeChart(@AuthenticationPrincipal UserDetails userDetails, Model model) {
        String username = userDetails.getUsername();
        User user = userService.findByUsername(username);

        var chartData = chartDataService.getTimeChartData();

        model.addAttribute("userInfo", user);
        model.addAttribute("x1Data1", chartData.get("x1Data1"));
        model.addAttribute("y1Data1", chartData.get("y1Data1"));
        model.addAttribute("y1Data2", chartData.get("y1Data2"));
        model.addAttribute("x2Data1", chartData.get("x2Data1"));
        model.addAttribute("y2Data1", chartData.get("y2Data1"));
        model.addAttribute("x3Data1", chartData.get("x3Data1"));
        model.addAttribute("y3Data1", chartData.get("y3Data1"));
        model.addAttribute("currentPage", "timeChart");

        return "timeChart";
    }

    /**
     * 词云页面（对应novelCloud视图）
     */
    @GetMapping("/novelCloud")
    public String novelCloud(@AuthenticationPrincipal UserDetails userDetails,
                             @RequestParam(required = false, defaultValue = "title") String dataType,
                           Model model) {
        String username = userDetails.getUsername();
        User user = userService.findByUsername(username);

        List<Map<String, Object>> wordCloudData;
        String dataTypeName = switch (dataType.toLowerCase()) {
            case "title" -> {
                // 基于小说名关键词的词云
                wordCloudData = chartDataService.getNovelNameWordCloudData();
                yield "书名";
            }
            case "author" -> {
                // 基于作者的词云
                wordCloudData = chartDataService.getAuthorWordCloudData();
                yield "作者";
            }
            default -> {
                // 默认基于类型的词云
                wordCloudData = chartDataService.getWordCloudData();
                yield "类型";
            }
        };

        model.addAttribute("userInfo", user);
        model.addAttribute("wordCloudData", wordCloudData);
        model.addAttribute("dataType", dataType);
        model.addAttribute("dataTypeName", dataTypeName);
        model.addAttribute("currentPage", "novelCloud");

        return "novelCloud";
    }

    /**
     * 作者图表页面（对应authorChart视图）
     */
    @GetMapping("/authorChart")
    public String authorChart(@AuthenticationPrincipal UserDetails userDetails, Model model) {
        String username = userDetails.getUsername();
        User user = userService.findByUsername(username);

        // 获取原有作者图表数据
        var chartData = chartDataService.getAuthorChartData();

        // 获取新的作者分析数据
        var authorAnalysisData = chartDataService.getAuthorAnalysisChartData();

        model.addAttribute("userInfo", user);
        model.addAttribute("xLine1", chartData.get("xLine1"));
        model.addAttribute("yLine1", chartData.get("yLine1"));
        model.addAttribute("pieData", chartData.get("pieData"));

        // 添加作者分析数据
        model.addAttribute("levelDistributionData", authorAnalysisData.get("levelDistributionData"));
        model.addAttribute("channelComparisonData", authorAnalysisData.get("channelComparisonData"));
        model.addAttribute("efficiencyAnalysisData", authorAnalysisData.get("efficiencyAnalysisData"));
        model.addAttribute("productivityData", authorAnalysisData.get("productivityData"));
        model.addAttribute("readFlowerData", authorAnalysisData.get("readFlowerData"));
        model.addAttribute("topAuthorsData", authorAnalysisData.get("topAuthorsData"));

        model.addAttribute("currentPage", "authorChart");

        return "authorChart";
    }

    /**
     * 频道对比分析页面
     */
    @GetMapping("/channelChart")
    public String channelChart(@AuthenticationPrincipal UserDetails userDetails, Model model) {
        String username = userDetails.getUsername();
        User user = userService.findByUsername(username);

        // 获取频道对比数据
        var chartData = chartDataService.getChannelComparisonChartData();

        model.addAttribute("userInfo", user);
        model.addAttribute("currentPage", "channelChart");
        model.addAttribute("themeData", chartData.get("channelThemeData"));
        model.addAttribute("flowerData", chartData.get("flowerTrendData"));
        model.addAttribute("metricsData", chartData.get("channelMetrics"));

        return "channelChart";
    }

}