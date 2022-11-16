package com.elastiSearchClient.config;

import org.springframework.context.annotation.Configuration;

@Configuration
public class ES_config {
    public final static String indexName = "device_index_main";
    public final static String elasticSearchHost = "localhost";
    public final static Integer elasticSearchPort = 9200;
    public final static String elasticSearchScheme = "http";


}
