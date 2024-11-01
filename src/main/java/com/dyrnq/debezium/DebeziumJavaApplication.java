package com.dyrnq.debezium;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
public class DebeziumJavaApplication {

    public static void main(String[] args) {
        new SpringApplicationBuilder(DebeziumJavaApplication.class)
                .web(WebApplicationType.NONE)
                .run(args);
    }
}
