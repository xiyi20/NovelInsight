package com.novelinsight.service;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * MySQL查询服务，对应Python中的query.py
 * 用于查询本地MySQL数据库 (novelData)
 */
@Service
public class MySqlQueryService {

    private final JdbcTemplate novelJdbcTemplate;
    private final NamedParameterJdbcTemplate novelNamedParameterJdbcTemplate;

    public MySqlQueryService(
            @Qualifier("novelJdbcTemplate") JdbcTemplate novelJdbcTemplate,
            @Qualifier("novelNamedParameterJdbcTemplate") NamedParameterJdbcTemplate novelNamedParameterJdbcTemplate) {
        this.novelJdbcTemplate = novelJdbcTemplate;
        this.novelNamedParameterJdbcTemplate = novelNamedParameterJdbcTemplate;
    }

    /**
     * 执行查询并返回结果列表
     */
    public List<Object[]> query(String sql, Object... params) {
        return novelJdbcTemplate.query(sql, (rs, rowNum) -> {
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
        return novelNamedParameterJdbcTemplate.query(sql, paramMap, (rs, rowNum) -> {
            int columnCount = rs.getMetaData().getColumnCount();
            Object[] row = new Object[columnCount];
            for (int i = 1; i <= columnCount; i++) {
                row[i - 1] = rs.getObject(i);
            }
            return row;
        });
    }

    /**
     * 执行更新操作
     */
    public int update(String sql, Object... params) {
        return novelJdbcTemplate.update(sql, params);
    }

    /**
     * 执行更新操作（使用命名参数）
     */
    public int updateWithNamedParams(String sql, Map<String, ?> paramMap) {
        return novelNamedParameterJdbcTemplate.update(sql, paramMap);
    }

    /**
     * 查询单个值
     */
    public <T> T queryForObject(String sql, Class<T> requiredType, Object... params) {
        return novelJdbcTemplate.queryForObject(sql, requiredType, params);
    }

    /**
     * 查询单行
     */
    public Map<String, Object> queryForMap(String sql, Object... params) {
        return novelJdbcTemplate.queryForMap(sql, params);
    }

    /**
     * 查询列表
     */
    public List<Map<String, Object>> queryForList(String sql, Object... params) {
        return novelJdbcTemplate.queryForList(sql, params);
    }
}