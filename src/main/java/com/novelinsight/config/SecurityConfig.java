package com.novelinsight.config;

import com.novelinsight.service.security.CustomPasswordEncoder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.AuthenticationSuccessHandler;
import org.springframework.security.web.csrf.CookieCsrfTokenRepository;

@Configuration
@EnableWebSecurity
public class SecurityConfig {

    private final CustomPasswordEncoder customPasswordEncoder;
    private final UserDetailsService userDetailsService;
    public SecurityConfig(CustomPasswordEncoder customPasswordEncoder, UserDetailsService userDetailsService) {
        this.customPasswordEncoder = customPasswordEncoder;
        this.userDetailsService = userDetailsService;
    }

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http
            .csrf(csrf -> csrf
                .csrfTokenRepository(CookieCsrfTokenRepository.withHttpOnlyFalse())
                .ignoringRequestMatchers("/h2-console/**") // 仅用于开发，生产环境应移除
            )
            .authorizeHttpRequests(authz -> authz
                .requestMatchers("/", "/app/login", "/app/register",
                        "/assets/**", "/static/**", "/webjars/**", "/favicon.ico").permitAll()
                .anyRequest().authenticated()
            )
            .formLogin(form -> form
                .loginPage("/app/login")
                .loginProcessingUrl("/app/login")
                .usernameParameter("loginName")
                .passwordParameter("loginPwd")
                .successHandler(authenticationSuccessHandler())
                .failureUrl("/app/login?error=true")
                .permitAll()
            )
            .logout(logout -> logout
                .logoutUrl("/app/logout")
                .logoutSuccessUrl("/app/login")
                .invalidateHttpSession(true)
                .deleteCookies("JSESSIONID", "remember-me")
                .permitAll()
            )
            .rememberMe(remember -> remember
                .key("uniqueAndSecretKeyForRememberMe")
                .tokenValiditySeconds(86400 * 30) // 30天
                .rememberMeParameter("remember-me")
                .userDetailsService(userDetailsService)
            )
            .sessionManagement(session -> session
                .maximumSessions(1)
                .maxSessionsPreventsLogin(false)
            );

        return http.build();
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return customPasswordEncoder;
    }

    @Bean
    public AuthenticationSuccessHandler authenticationSuccessHandler() {
        return (request, response, authentication) -> {
            // 在session中存储用户名，保持与Django兼容
            request.getSession().setAttribute("username", authentication.getName());

            // 登录成功后重定向到首页
            response.sendRedirect("/app/index");
        };
    }
}