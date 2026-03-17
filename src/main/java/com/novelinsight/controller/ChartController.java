package com.novelinsight.controller;

import com.novelinsight.model.User;
import com.novelinsight.service.ChartDataService;
import com.novelinsight.service.PublicDataService;
import com.novelinsight.service.UserService;
import jakarta.servlet.http.HttpServletRequest;
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
     */
    @GetMapping("/typeChart")
    public String typeChart(@AuthenticationPrincipal UserDetails userDetails, Model model, HttpServletRequest request) {
        String username = userDetails.getUsername();
        User user = userService.findByUsername(username);

        var chartData = chartDataService.getTypeChartData();

        model.addAttribute("userInfo", user);
        model.addAttribute("xLine1", chartData.get("xLine1"));
        model.addAttribute("yLine1", chartData.get("yLine1"));
        model.addAttribute("yLine2", chartData.get("yLine2"));
        model.addAttribute("xBar1", chartData.get("xBar1"));
        model.addAttribute("yBar1", chartData.get("yBar1"));
        model.addAttribute("pieData1", chartData.get("pieData1"));
        model.addAttribute("currentPage", "typeChart");

        return "typeChart";
    }

    /**
     * 小说信息图表页面（对应infoChart视图）
     */
    @GetMapping("/infoChart")
    public String infoChart(@AuthenticationPrincipal UserDetails userDetails,
                           @RequestParam(required = false) String typeName,
                           Model model, HttpServletRequest request) {
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
    public String timeChart(@AuthenticationPrincipal UserDetails userDetails, Model model, HttpServletRequest request) {
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
                           @RequestParam(required = false, defaultValue = "type") String dataType,
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
    public String authorChart(@AuthenticationPrincipal UserDetails userDetails, Model model, HttpServletRequest request) {
        String username = userDetails.getUsername();
        User user = userService.findByUsername(username);

        var chartData = chartDataService.getAuthorChartData();

        model.addAttribute("userInfo", user);
        model.addAttribute("xLine1", chartData.get("xLine1"));
        model.addAttribute("yLine1", chartData.get("yLine1"));
        model.addAttribute("pieData", chartData.get("pieData"));
        model.addAttribute("currentPage", "authorChart");

        return "authorChart";
    }
}