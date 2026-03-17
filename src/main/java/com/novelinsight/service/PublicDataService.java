package com.novelinsight.service;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 公共数据服务，对应Python中的getPublicData.py
 */
@Service
public class PublicDataService {

    private final HiveService hiveService;

    public PublicDataService(HiveService hiveService) {
        this.hiveService = hiveService;
    }

    /**
     * 获取所有小说数据
     */
    public List<Object[]> getNovelData() {
        String sql = "SELECT * FROM novelData";
        return hiveService.query(sql);
    }

    /**
     * 获取过滤和排序后的小说数据（带分页）
     *
     * @param typeFilter   类型过滤（可选）
     * @param authorFilter 作者过滤（可选）
     * @param novelFilter  小说名过滤（可选）
     * @param sortField    排序字段（可选，默认阅读量）
     * @param sortOrder    排序顺序（ASC/DESC，默认DESC）
     * @param page         页码（从0开始）
     * @param size         每页大小
     * @return 分页的小说数据
     */
    public Page<Object[]> getFilteredNovelData(String typeFilter, String authorFilter, String novelFilter,
                                               String sortField, String sortOrder, int page, int size) {
        // 1. 提取公共的 WHERE 子句构建逻辑
        StringBuilder whereClause = new StringBuilder(" FROM novelData WHERE 1=1");
        List<Object> params = new ArrayList<>();

        if (hasText(typeFilter)) {
            whereClause.append(" AND type LIKE ?");
            params.add("%" + typeFilter.trim() + "%");
        }
        if (hasText(authorFilter)) {
            whereClause.append(" AND author LIKE ?");
            params.add("%" + authorFilter.trim() + "%");
        }
        if (hasText(novelFilter)) {
            whereClause.append(" AND title LIKE ?");
            params.add("%" + novelFilter.trim() + "%");
        }

        // 2. 验证并构建排序子句
        String orderBy = " ORDER BY " + validateSortField(sortField) + " " + ("ASC".equalsIgnoreCase(sortOrder) ? "ASC" : "DESC");

        // 3. 异步并行执行：总记录数查询 和 数据分页查询
        CompletableFuture<Long> countFuture = CompletableFuture.supplyAsync(() -> {
            String countSql = "SELECT COUNT(*)" + whereClause;
            return hiveService.queryForObject(countSql, Long.class, params.toArray());
        });

        CompletableFuture<List<Object[]>> dataFuture = CompletableFuture.supplyAsync(() -> {
            // Spark SQL/Hive 分页方案：使用ROW_NUMBER()窗口函数
            long offset = (long) page * size;
            long endRow = offset + size;
            long startRow = offset + 1; // ROW_NUMBER从1开始

            // 构建基础查询（去除开头的" FROM"）
            String baseFromWhere = whereClause.toString();
            // 去掉开头的空格和"FROM"前的空格，得到"novelData WHERE 1=1..."
            String fromWherePart = baseFromWhere.trim();
            if (fromWherePart.startsWith("FROM ")) {
                fromWherePart = fromWherePart.substring(5);
            }

            String dataSql = "SELECT * FROM (" +
                    "SELECT *, ROW_NUMBER() OVER (" + orderBy + ") as rn " +
                    "FROM " + fromWherePart + ") t WHERE rn >= " + startRow + " AND rn <= " + endRow;

            return hiveService.query(dataSql, params.toArray());
        });

        // 4. 等待两者完成
        try {
            CompletableFuture.allOf(countFuture, dataFuture).join();
            return new PageImpl<>(dataFuture.get(), PageRequest.of(page, size), countFuture.get());
        } catch (Exception e) {
            throw new RuntimeException("集群查询超时或失败", e);
        }
    }

    // 辅助：检查字符串是否为空
    private boolean hasText(String str) {
        return str != null && !str.trim().isEmpty();
    }

    // 辅助：SQL注入防御及字段转换
    private String validateSortField(String sortField) {
        if (sortField == null) return "allRead";
        return switch (sortField.trim().toLowerCase()) {
            case "wordnum" -> "wordNum";
            case "allflower" -> "allFlower";
            case "reward" -> "reward";
            default -> "allRead";
        };
    }

    /**
     * 获取阅读量Top小说
     */
    public List<Object[]> getTopRead() {
        String sql = "SELECT * FROM TopRead";
        return hiveService.query(sql);
    }

    /**
     * 获取打赏Top小说
     */
    public List<Object[]> getTopReward() {
        String sql = "SELECT * FROM TopReward";
        return hiveService.query(sql);
    }

    /**
     * 获取类型平均阅读量
     */
    public List<Object[]> getTypeAvgRead() {
        String sql = "SELECT * FROM typeAvgRead";
        return hiveService.query(sql);
    }

    /**
     * 获取月票Top
     */
    public List<Object[]> getTopMTicket() {
        String sql = "SELECT * FROM TopMTicket";
        return hiveService.query(sql);
    }

    /**
     * 获取小说类型最大阅读量和收藏量
     */
    public List<Object[]> getNovelTypeMaxRF() {
        String sql = "SELECT * FROM novelTypeMaxRF";
        return hiveService.query(sql);
    }

    /**
     * 获取小说最大分享
     */
    public List<Object[]> getNovelMaxShare() {
        String sql = "SELECT * FROM novelMaxShare";
        return hiveService.query(sql);
    }

    /**
     * 获取小说类型统计
     */
    public List<Object[]> getNovelType() {
        String sql = "SELECT * FROM novelType";
        return hiveService.query(sql);
    }

    /**
     * 获取小说阅读范围分布
     */
    public List<Object[]> getNovelReadRange() {
        String sql = "SELECT * FROM novelReadRange";
        return hiveService.query(sql);
    }

    /**
     * 获取小说字数范围分布
     */
    public List<Object[]> getNovelWordNumRange() {
        String sql = "SELECT * FROM novelWordNumRange";
        return hiveService.query(sql);
    }

    /**
     * 获取小说鲜花Top
     */
    public List<Object[]> getNovelFlowerTop() {
        String sql = "SELECT * FROM novelFlowerTop";
        return hiveService.query(sql);
    }

    /**
     * 获取作者日更Top
     */
    public List<Object[]> getAuthorDayTop() {
        String sql = "SELECT * FROM authorDayTop";
        return hiveService.query(sql);
    }

    /**
     * 获取作者最大字数
     */
    public List<Object[]> getAuthorMaxWord() {
        String sql = "SELECT * FROM authorMaxWord";
        return hiveService.query(sql);
    }

    /**
     * 获取小说月度数量统计
     */
    public List<Object[]> getNovelMonthCount() {
        String sql = "SELECT * FROM novelMonthCount";
        return hiveService.query(sql);
    }

    /**
     * 获取小说月度阅读统计
     */
    public List<Object[]> getNovelMonthRead() {
        String sql = "SELECT * FROM novelMonthRead";
        return hiveService.query(sql);
    }

    /**
     * 获取小说年度阅读统计
     */
    public List<Object[]> getNovelYearRead() {
        String sql = "SELECT * FROM novelYearRead";
        return hiveService.query(sql);
    }
}