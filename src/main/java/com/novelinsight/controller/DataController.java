package com.novelinsight.controller;

import com.novelinsight.model.User;
import com.novelinsight.service.ChartDataService;
import com.novelinsight.service.HistoryService;
import com.novelinsight.service.PublicDataService;
import com.novelinsight.service.UserService;
import org.springframework.data.domain.Page;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;

@Controller
@RequestMapping("/app")
public class DataController {

    private final UserService userService;
    private final PublicDataService publicDataService;
    private final HistoryService historyService;
    private final ChartDataService chartDataService;

    public DataController(UserService userService,
                         PublicDataService publicDataService,
                         HistoryService historyService,
                         ChartDataService chartDataService) {
        this.userService = userService;
        this.publicDataService = publicDataService;
        this.historyService = historyService;
        this.chartDataService = chartDataService;
    }

    /**
     * 表格数据页面（对应tableData视图）
     */
    @GetMapping("/tableData")
    public String tableData(@AuthenticationPrincipal UserDetails userDetails,
                           @RequestParam(defaultValue = "1") int page,
                           @RequestParam(defaultValue = "15") int size,
                           @RequestParam(required = false) String typeFilter,
                           @RequestParam(required = false) String authorFilter,
                           @RequestParam(required = false) String novelFilter,
                           @RequestParam(required = false) String sortField,
                           @RequestParam(required = false) String sortOrder,
                           Model model) {
        String username = userDetails.getUsername();
        User user = userService.findByUsername(username);

        // 转换为0-based页码（Spring Data Page使用0-based）
        int pageZeroBased = page - 1;

        // 使用物理分页获取数据
        Page<Object[]> novelPage = publicDataService.getFilteredNovelData(
                typeFilter, authorFilter, novelFilter, sortField, sortOrder,
                pageZeroBased, size
        );

        model.addAttribute("userInfo", user);
        model.addAttribute("articles_page", novelPage);
        model.addAttribute("currentPage", "tableData");

        // 传递过滤参数，以便在页面中保持状态
        model.addAttribute("typeFilter", typeFilter != null ? typeFilter : "");
        model.addAttribute("authorFilter", authorFilter != null ? authorFilter : "");
        model.addAttribute("novelFilter", novelFilter != null ? novelFilter : "");
        model.addAttribute("sortField", sortField != null ? sortField : "allRead");
        model.addAttribute("sortOrder", sortOrder != null ? sortOrder : "DESC");

        return "tableData";
    }

    /**
     * 收藏数据页面（对应collectData视图）
     */
    @GetMapping("/collectData")
    public String collectData(@AuthenticationPrincipal UserDetails userDetails, Model model) {
        String username = userDetails.getUsername();
        User user = userService.findByUsername(username);

        // 获取用户收藏（历史记录）
        List<Object[]> collectList = historyService.getUserHistory(user);

        model.addAttribute("userInfo", user);
        model.addAttribute("collectList", collectList);
        model.addAttribute("currentPage", "collectData");

        return "collectData";
    }

    /**
     * 添加收藏（对应addHistory视图）
     */
    @GetMapping("/addHistory/{novelId}")
    public String addHistory(@AuthenticationPrincipal UserDetails userDetails,
                            @PathVariable String novelId) {
        String username = userDetails.getUsername();
        User user = userService.findByUsername(username);
        historyService.addHistory(user, novelId);
        return "redirect:/app/collectData";
    }

    /**
     * 删除收藏（对应delHistory视图）
     */
    @GetMapping("/delHistory/{novelId}")
    public String delHistory(@AuthenticationPrincipal UserDetails userDetails,
                            @PathVariable String novelId) {
        String username = userDetails.getUsername();
        User user = userService.findByUsername(username);

        historyService.deleteHistory(user, novelId);

        return "redirect:/app/collectData";
    }
}