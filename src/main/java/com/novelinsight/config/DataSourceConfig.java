package com.novelinsight.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;

@Configuration
@EnableTransactionManagement
@EnableJpaRepositories(
        basePackages = "com.novelinsight.repository",
        entityManagerFactoryRef = "novelEntityManager",
        transactionManagerRef = "novelTransactionManager"
)
public class DataSourceConfig {

    // 主数据源：本地MySQL (novelData数据库)
    @Primary
    @Bean(name = "novelDataSource")
    public HikariDataSource novelDataSource(
            @Value("${spring.datasource.novel.url}") String url,
            @Value("${spring.datasource.novel.username}") String username,
            @Value("${spring.datasource.novel.password}") String password,
            @Value("${spring.datasource.novel.driver-class-name}") String driverClassName) {

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);
        config.setDriverClassName(driverClassName);
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(5);
        config.setConnectionTimeout(30000);

        return new HikariDataSource(config);
    }

    // 集群MySQL数据源 (bigdata数据库)
    @Bean(name = "bigdataDataSource")
    public HikariDataSource bigdataDataSource(
            @Value("${spring.datasource.bigdata.url}") String url,
            @Value("${spring.datasource.bigdata.username}") String username,
            @Value("${spring.datasource.bigdata.password}") String password,
            @Value("${spring.datasource.bigdata.driver-class-name}") String driverClassName) {

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);
        config.setDriverClassName(driverClassName);
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(5);
        config.setConnectionTimeout(30000);

        return new HikariDataSource(config);
    }

    // Hive数据源
    @Bean(name = "hiveDataSource")
    public HikariDataSource hiveDataSource(
            @Value("${spring.datasource.hive.url}") String url,
            @Value("${spring.datasource.hive.username}") String username,
            @Value("${spring.datasource.hive.driver-class-name}") String driverClassName) {

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setDriverClassName(driverClassName);
        config.setMaximumPoolSize(5);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(60000);

        return new HikariDataSource(config);
    }

    // JdbcTemplate for novel (本地MySQL)
    @Bean(name = "novelJdbcTemplate")
    public JdbcTemplate novelJdbcTemplate(@Qualifier("novelDataSource") DataSource novelDataSource) {
        return new JdbcTemplate(novelDataSource);
    }

    @Bean(name = "novelNamedParameterJdbcTemplate")
    public NamedParameterJdbcTemplate novelNamedParameterJdbcTemplate(@Qualifier("novelDataSource") DataSource novelDataSource) {
        return new NamedParameterJdbcTemplate(novelDataSource);
    }

    // JdbcTemplate for bigdata
    @Bean(name = "bigdataJdbcTemplate")
    public JdbcTemplate bigdataJdbcTemplate(@Qualifier("bigdataDataSource") DataSource bigdataDataSource) {
        return new JdbcTemplate(bigdataDataSource);
    }

    @Bean(name = "bigdataNamedParameterJdbcTemplate")
    public NamedParameterJdbcTemplate bigdataNamedParameterJdbcTemplate(@Qualifier("bigdataDataSource") DataSource bigdataDataSource) {
        return new NamedParameterJdbcTemplate(bigdataDataSource);
    }

    // JdbcTemplate for hive
    @Bean(name = "hiveJdbcTemplate")
    public JdbcTemplate hiveJdbcTemplate(@Qualifier("hiveDataSource") DataSource hiveDataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(hiveDataSource);
        jdbcTemplate.setQueryTimeout(30); // 设置查询超时30秒
        return jdbcTemplate;
    }

    @Bean(name = "hiveNamedParameterJdbcTemplate")
    public NamedParameterJdbcTemplate hiveNamedParameterJdbcTemplate(@Qualifier("hiveDataSource") DataSource hiveDataSource) {
        return new NamedParameterJdbcTemplate(hiveDataSource);
    }

    // EntityManager for novel datasource
    @Primary
    @Bean(name = "novelEntityManager")
    public LocalContainerEntityManagerFactoryBean novelEntityManager(@Qualifier("novelDataSource") DataSource novelDataSource) {
        LocalContainerEntityManagerFactoryBean em = new LocalContainerEntityManagerFactoryBean();
        em.setDataSource(novelDataSource);
        em.setPackagesToScan("com.novelinsight.model");

        HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
        em.setJpaVendorAdapter(vendorAdapter);

        return em;
    }

    // TransactionManager for novel datasource
    @Primary
    @Bean(name = "novelTransactionManager")
    public PlatformTransactionManager novelTransactionManager(@Qualifier("novelEntityManager") LocalContainerEntityManagerFactoryBean novelEntityManager) {
        JpaTransactionManager transactionManager = new JpaTransactionManager();
        transactionManager.setEntityManagerFactory(novelEntityManager.getObject());
        return transactionManager;
    }
}