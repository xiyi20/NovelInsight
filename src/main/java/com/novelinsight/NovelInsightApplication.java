package com.novelinsight;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class NovelInsightApplication {

    public static void main(String[] args) {
        SpringApplication.run(NovelInsightApplication.class, args);
    }

}