package com.novelinsight.service;

import com.novelinsight.model.User;
import com.novelinsight.repository.UserRepository;
import com.novelinsight.service.security.CustomPasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class UserService {

    private final UserRepository userRepository;
    private final CustomPasswordEncoder passwordEncoder;

    public UserService(UserRepository userRepository, CustomPasswordEncoder passwordEncoder) {
        this.userRepository = userRepository;
        this.passwordEncoder = passwordEncoder;
    }

    /**
     * 用户注册
     */
    @Transactional
    public User register(String username, String password) {
        // 用户名格式验证
        validateUsernameFormat(username);

        if (userRepository.existsByUsername(username)) {
            throw new IllegalArgumentException("用户名已注册");
        }

        // 密码强度验证
        validatePasswordStrength(password);

        User user = new User(username, passwordEncoder.encode(password));
        return userRepository.save(user);
    }

    /**
     * 验证用户凭据
     */
    public boolean authenticate(String username, String password) {
        return userRepository.findByUsername(username)
                .map(user -> {
                    boolean matches = passwordEncoder.matches(password, user.getPassword());
                    if (matches && passwordEncoder.needsUpgrade(user.getPassword())) {
                        // 密码需要升级（明文存储）
                        upgradePassword(user, password);
                    }
                    return matches;
                })
                .orElse(false);
    }

    /**
     * 升级用户密码（从明文升级到BCrypt）
     */
    @Transactional
    public void upgradePassword(User user, String rawPassword) {
        user.setPassword(passwordEncoder.encode(rawPassword));
        userRepository.save(user);
    }

    /**
     * 修改密码
     */
    @Transactional
    public boolean changePassword(String username, String oldPassword, String newPassword) {
        User user = userRepository.findByUsername(username)
                .orElseThrow(() -> new IllegalArgumentException("用户不存在"));

        if (!passwordEncoder.matches(oldPassword, user.getPassword())) {
            return false;
        }

        user.setPassword(passwordEncoder.encode(newPassword));
        userRepository.save(user);
        return true;
    }

    /**
     * 根据用户名查找用户
     */
    public User findByUsername(String username) {
        return userRepository.findByUsername(username)
                .orElseThrow(() -> new IllegalArgumentException("用户不存在"));
    }

    /**
     * 用户名格式验证
     * 1. 长度3-20个字符
     * 2. 只能包含字母、数字、下划线
     */
    private void validateUsernameFormat(String username) {
        if (username == null || username.length() < 3 || username.length() > 20) {
            throw new IllegalArgumentException("用户名长度需在3-20个字符之间");
        }

        if (!username.matches("^[a-zA-Z0-9_]+$")) {
            throw new IllegalArgumentException("用户名只能包含字母、数字和下划线");
        }
    }

    /**
     * 密码强度验证
     * 1. 至少8个字符
     * 2. 至少包含一个字母和一个数字
     */
    private void validatePasswordStrength(String password) {
        if (password == null || password.length() < 8) {
            throw new IllegalArgumentException("密码长度至少8个字符");
        }

        boolean hasLetter = false;
        boolean hasDigit = false;
        for (char c : password.toCharArray()) {
            if (Character.isLetter(c)) {
                hasLetter = true;
            } else if (Character.isDigit(c)) {
                hasDigit = true;
            }
        }

        if (!hasLetter || !hasDigit) {
            throw new IllegalArgumentException("密码必须包含字母和数字");
        }
    }
}