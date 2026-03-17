package com.novelinsight.service;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * Hive查询服务，对应Python中的queryhive.py
 */
@Service
public class HiveService {

    private final JdbcTemplate hiveJdbcTemplate;
    private final NamedParameterJdbcTemplate hiveNamedParameterJdbcTemplate;

    public HiveService(
            @Qualifier("hiveJdbcTemplate") JdbcTemplate hiveJdbcTemplate,
            @Qualifier("hiveNamedParameterJdbcTemplate") NamedParameterJdbcTemplate hiveNamedParameterJdbcTemplate) {
        this.hiveJdbcTemplate = hiveJdbcTemplate;
        this.hiveNamedParameterJdbcTemplate = hiveNamedParameterJdbcTemplate;
    }

    /**
     * 执行查询并返回结果列表
     * @param sql SQL语句
     * @param params 参数列表
     * @return 查询结果列表，每行作为一个Object数组
     */
    public List<Object[]> query(String sql, Object... params) {
        return hiveJdbcTemplate.query(sql, (rs, rowNum) -> {
            int columnCount = rs.getMetaData().getColumnCount();
            Object[] row = new Object[columnCount];
            for (int i = 1; i <= columnCount; i++) {
                row[i - 1] = rs.getObject(i);
            }
            return row;
        }, params);
    }

    /**
     * 执行查询并返回结果列表（使用命名参数）
     */
    public List<Object[]> queryWithNamedParams(String sql, Map<String, ?> paramMap) {
        return hiveNamedParameterJdbcTemplate.query(sql, paramMap, (rs, rowNum) -> {
            int columnCount = rs.getMetaData().getColumnCount();
            Object[] row = new Object[columnCount];
            for (int i = 1; i <= columnCount; i++) {
                row[i - 1] = rs.getObject(i);
            }
            return row;
        });
    }

    /**
     * 执行更新操作（INSERT/UPDATE/DELETE）
     */
    public int update(String sql, Object... params) {
        return hiveJdbcTemplate.update(sql, params);
    }

    /**
     * 执行更新操作（使用命名参数）
     */
    public int updateWithNamedParams(String sql, Map<String, ?> paramMap) {
        return hiveNamedParameterJdbcTemplate.update(sql, paramMap);
    }

    /**
     * 查询单个值
     */
    public <T> T queryForObject(String sql, Class<T> requiredType, Object... params) {
        return hiveJdbcTemplate.queryForObject(sql, requiredType, params);
    }

    /**
     * 查询单行
     */
    public Map<String, Object> queryForMap(String sql, Object... params) {
        return hiveJdbcTemplate.queryForMap(sql, params);
    }

    /**
     * 查询列表
     */
    public List<Map<String, Object>> queryForList(String sql, Object... params) {
        return hiveJdbcTemplate.queryForList(sql, params);
    }
}