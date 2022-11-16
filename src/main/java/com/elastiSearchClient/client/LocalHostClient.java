package com.elastiSearchClient.client;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import static com.elastiSearchClient.config.ES_config.elasticSearchPort;
import static com.elastiSearchClient.config.ES_config.elasticSearchHost;
import static com.elastiSearchClient.config.ES_config.elasticSearchScheme;

@Configuration
@Component
public class LocalHostClient {
    public static RestHighLevelClient create() {
        return new RestHighLevelClient(RestClient.builder(
                new HttpHost(elasticSearchHost, elasticSearchPort, elasticSearchScheme)
        ));
    }
}
