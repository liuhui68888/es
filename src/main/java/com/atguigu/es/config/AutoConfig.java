package com.atguigu.es.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/*************************************************
                时间: 2021-01-02
                作者: 刘  辉
                描述: 
  ************************************************/
@Configuration
public class AutoConfig {

    @Bean
    public RestHighLevelClient getHighLevelClient(){
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));
        return client;
    }


}
