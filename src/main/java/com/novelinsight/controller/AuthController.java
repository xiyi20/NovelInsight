package com.novelinsight.controller;

import com.novelinsight.service.UserService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.WebAttributes;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

@Controller
@RequestMapping("/app")
public class AuthController {

    private final UserService userService;

    public AuthController(UserService userService) {
        this.userService = userService;
    }

    @GetMapping("/login")
    public String loginPage(HttpServletRequest request, Model model) {
        // 如果用户已登录，重定向到首页
        HttpSession session = request.getSession(false);
        if (session != null && session.getAttribute("username") != null) {
            return "redirect:/app/index";
        }

        // 检查Spring Security认证异常
        if (session != null) {
            AuthenticationException ex = (AuthenticationException) session.getAttribute(WebAttributes.AUTHENTICATION_EXCEPTION);
            if (ex != null) {
                String errorMessage = getErrorMessage(ex);
                model.addAttribute("errorMessage", errorMessage);
                // 清除异常，避免重复显示
                session.removeAttribute(WebAttributes.AUTHENTICATION_EXCEPTION);
            }
        }

        // 兼容旧的error参数方式
        String error = request.getParameter("error");
        if (error != null && model.getAttribute("errorMessage") == null) {
            model.addAttribute("errorMessage", "用户名或密码错误");
        }

        // 显示注册成功消息
        if (session != null && session.getAttribute("registrationSuccess") != null) {
            model.addAttribute("successMessage", "恭喜！注册成功，请登录");
            session.removeAttribute("registrationSuccess");
        }

        return "login";
    }

    @GetMapping("/register")
    public String registerPage() {
        return "register";
    }

    @PostMapping("/register")
    public String register(@RequestParam String username,
                           @RequestParam String password,
                           @RequestParam String ckPassword,
                           RedirectAttributes redirectAttributes) {

        if (!password.equals(ckPassword)) {
            redirectAttributes.addFlashAttribute("errorMessage", "确认密码失败，请重新输入");
            return "redirect:/app/register";
        }

        try {
            userService.register(username, password);
            redirectAttributes.addFlashAttribute("successMessage", "恭喜！注册成功");
            return "redirect:/app/login";
        } catch (IllegalArgumentException e) {
            redirectAttributes.addFlashAttribute("errorMessage", e.getMessage());
            return "redirect:/app/register";
        }
    }

    @GetMapping("/logout")
    public String logout(HttpServletRequest request) {
        HttpSession session = request.getSession(false);
        if (session != null) {
            session.invalidate();
        }
        return "redirect:/app/login";
    }

    /**
     * 将Spring Security认证异常转换为友好的错误消息
     */
    private String getErrorMessage(AuthenticationException ex) {
        String errorMessage = "登录失败：";
        String exceptionMessage = ex.getMessage();

        if (exceptionMessage != null) {
            if (exceptionMessage.contains("Bad credentials")) {
                return "用户名或密码错误";
            } else if (exceptionMessage.contains("User is disabled")) {
                return "用户账户已被禁用";
            } else if (exceptionMessage.contains("User account is locked")) {
                return "用户账户已被锁定";
            } else if (exceptionMessage.contains("User account has expired")) {
                return "用户账户已过期";
            } else if (exceptionMessage.contains("credentials have expired")) {
                return "密码已过期";
            } else {
                return errorMessage + exceptionMessage;
            }
        }

        return "用户名或密码错误";
    }
}