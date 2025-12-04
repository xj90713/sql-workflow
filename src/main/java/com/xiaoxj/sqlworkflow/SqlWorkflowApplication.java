package com.xiaoxj.sqlworkflow;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SqlWorkflowApplication {
    public static void main(String[] args) {
        SpringApplication.run(SqlWorkflowApplication.class, args);
    }
}
