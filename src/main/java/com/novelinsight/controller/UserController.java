package com.novelinsight.controller;

import com.novelinsight.model.User;
import com.novelinsight.service.UserService;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

@Controller
@RequestMapping("/app")
public class UserController {

    private final UserService userService;

    public UserController(UserService userService) {
        this.userService = userService;
    }

    /**
     * 个人信息页面（对应selfInfo视图）
     */
    @GetMapping("/selfInfo")
    public String selfInfo(@AuthenticationPrincipal UserDetails userDetails, Model model, HttpServletRequest request) {
        String username = userDetails.getUsername();
        User user = userService.findByUsername(username);

        model.addAttribute("userInfo", user);
        model.addAttribute("currentPage", "selfInfo");

        return "selfInfo";
    }

    /**
     * 修改密码（对应selfInfo的POST请求）
     */
    @PostMapping("/selfInfo")
    public String changePassword(@AuthenticationPrincipal UserDetails userDetails,
                                @RequestParam String oldPwd,
                                @RequestParam String newPwd,
                                @RequestParam String ckdPwd,
                                RedirectAttributes redirectAttributes, HttpServletRequest request) {
        String username = userDetails.getUsername();

        boolean success = userService.changePassword(username, oldPwd, newPwd);

        if (success) {
            redirectAttributes.addFlashAttribute("successMessage", "密码修改成功");
        } else {
            redirectAttributes.addFlashAttribute("errorMessage", "原密码错误或新密码不匹配");
        }

        return "redirect:/app/selfInfo";
    }
}