package com.novelinsight.model;

import jakarta.persistence.*;
import lombok.Data;

@Entity
@Table(name = "history")
@Data
public class History {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id", nullable = false)
    private User user;

    @Column(name = "novelId", nullable = false, length = 255)
    private String novelId;

    @Column(name = "count", nullable = false)
    private Integer count = 1;

    // 无参构造函数
    public History() {
    }

    // 构造函数
    public History(User user, String novelId) {
        this.user = user;
        this.novelId = novelId;
        this.count = 1;
    }

    public History(User user, String novelId, Integer count) {
        this.user = user;
        this.novelId = novelId;
        this.count = count;
    }
}