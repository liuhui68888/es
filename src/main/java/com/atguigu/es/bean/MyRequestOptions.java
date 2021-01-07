package com.atguigu.es.bean;

import org.elasticsearch.client.RequestOptions;

/*************************************************
                时间: 2021-01-02
                作者: 刘  辉
                描述: 
  ************************************************/
public class MyRequestOptions {
    public static final RequestOptions COMMON_OPTIONS;
    static {
        //设置请求头信息，安全设置
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.addHeader("Authorization", "Bearer ");
        COMMON_OPTIONS = builder.build();
    }
}
