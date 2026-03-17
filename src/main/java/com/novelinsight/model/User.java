package com.novelinsight.model;

import jakarta.persistence.*;
import lombok.Data;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import java.time.LocalDate;

@Entity
@Table(name = "user")
@EntityListeners(AuditingEntityListener.class)
@Data
public class User {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "username", nullable = false, unique = true, length = 255)
    private String username;

    @Column(name = "password", nullable = false, length = 255)
    private String password;

    @CreatedDate
    @Column(name = "createTime", nullable = false)
    private LocalDate createTime;

    // 无参构造函数
    public User() {
    }

    // 全参构造函数
    public User(String username, String password) {
        this.username = username;
        this.password = password;
        this.createTime = LocalDate.now();
    }
}