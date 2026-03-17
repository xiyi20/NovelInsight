package com.novelinsight.service.security;

import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

/**
 * 自定义密码编码器，支持明文密码迁移到BCrypt
 * 1. 对于新用户和已迁移用户：使用BCrypt，格式为 {bcrypt}...
 * 2. 对于旧用户（明文密码）：验证后自动升级为BCrypt
 */
@Component
public class CustomPasswordEncoder implements PasswordEncoder {

    private static final String BCRYPT_PREFIX = "{bcrypt}";
    private static final String NOOP_PREFIX = "{noop}";
    private final BCryptPasswordEncoder bcryptEncoder = new BCryptPasswordEncoder();

    @Override
    public String encode(CharSequence rawPassword) {
        // 新密码使用BCrypt编码，并添加 {bcrypt} 前缀
        return BCRYPT_PREFIX + bcryptEncoder.encode(rawPassword);
    }

    @Override
    public boolean matches(CharSequence rawPassword, String encodedPassword) {
        // 处理 {bcrypt} 前缀的密码
        if (encodedPassword.startsWith(BCRYPT_PREFIX)) {
            String bcryptHash = encodedPassword.substring(BCRYPT_PREFIX.length());
            return bcryptEncoder.matches(rawPassword, bcryptHash);
        }

        // 处理 {noop} 前缀的密码（明文）
        if (encodedPassword.startsWith(NOOP_PREFIX)) {
            String plainPassword = encodedPassword.substring(NOOP_PREFIX.length());
            return plainPassword.equals(rawPassword.toString());
        }

        // 处理没有前缀的密码（兼容旧数据）
        // 先尝试作为BCrypt密码验证（没有前缀的BCrypt）
        if (bcryptEncoder.matches(rawPassword, encodedPassword)) {
            return true;
        }

        // 最后尝试明文匹配
        return encodedPassword.equals(rawPassword.toString());
    }

    /**
     * 判断密码是否需要升级（如果是明文存储）
     */
    public boolean needsUpgrade(String encodedPassword) {
        // 如果密码已经有 {bcrypt} 前缀，不需要升级
        if (encodedPassword.startsWith(BCRYPT_PREFIX)) {
            return false;
        }

        // 如果是有效的BCrypt哈希（即使没有前缀），不需要升级
        if (isBCryptHash(encodedPassword)) {
            return false;
        }

        // 其他情况（明文或未知格式）需要升级
        return true;
    }

    /**
     * 检查字符串是否是BCrypt哈希
     */
    private boolean isBCryptHash(String encodedPassword) {
        // BCrypt哈希格式: $2[abxy]$[0-9]{2}\$[./A-Za-z0-9]{53}
        return encodedPassword != null &&
               encodedPassword.length() == 60 &&
               encodedPassword.startsWith("$2") &&
               (encodedPassword.charAt(2) == 'a' || encodedPassword.charAt(2) == 'b' ||
                encodedPassword.charAt(2) == 'y' || encodedPassword.charAt(2) == 'x') &&
               encodedPassword.charAt(3) == '$' &&
               Character.isDigit(encodedPassword.charAt(4)) &&
               Character.isDigit(encodedPassword.charAt(5)) &&
               encodedPassword.charAt(6) == '$';
    }
}