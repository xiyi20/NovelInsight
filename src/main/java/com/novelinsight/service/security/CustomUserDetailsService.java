package com.novelinsight.service.security;

import com.novelinsight.model.User;
import com.novelinsight.repository.UserRepository;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;

import java.util.Collections;

@Service
public class CustomUserDetailsService implements UserDetailsService {

    private final UserRepository userRepository;

    public CustomUserDetailsService(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        User user = userRepository.findByUsername(username)
                .orElseThrow(() -> new UsernameNotFoundException("用户不存在: " + username));

        // 确保密码有正确的前缀格式
        String formattedPassword = formatPasswordForSpringSecurity(user.getPassword());

        return new org.springframework.security.core.userdetails.User(
                user.getUsername(),
                formattedPassword,
                Collections.singletonList(new SimpleGrantedAuthority("ROLE_USER"))
        );
    }

    /**
     * 格式化密码以便Spring Security的DelegatingPasswordEncoder能够识别
     * 1. 如果密码已有 {id} 前缀，保持原样
     * 2. 如果是BCrypt哈希（以$2开头），添加 {bcrypt} 前缀
     * 3. 否则作为明文密码，添加 {noop} 前缀
     */
    private String formatPasswordForSpringSecurity(String password) {
        if (password == null) {
            return password;
        }

        // 如果已经有 {id} 前缀，保持原样
        if (password.startsWith("{")) {
            return password;
        }

        // 检查是否是BCrypt哈希
        if (isBCryptHash(password)) {
            return "{bcrypt}" + password;
        }

        // 否则作为明文密码
        return "{noop}" + password;
    }

    /**
     * 检查字符串是否是BCrypt哈希
     */
    private boolean isBCryptHash(String password) {
        // BCrypt哈希格式: $2[abxy]$[0-9]{2}\$[./A-Za-z0-9]{53}
        return password != null &&
               password.length() == 60 &&
               password.startsWith("$2") &&
               (password.charAt(2) == 'a' || password.charAt(2) == 'b' ||
                password.charAt(2) == 'y' || password.charAt(2) == 'x') &&
               password.charAt(3) == '$' &&
               Character.isDigit(password.charAt(4)) &&
               Character.isDigit(password.charAt(5)) &&
               password.charAt(6) == '$';
    }
}