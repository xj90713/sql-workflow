package com.xiaoxj.sqlworkflow.config;

import com.xiaoxj.sqlworkflow.core.DolphinClient;
import com.xiaoxj.sqlworkflow.remote.DolphinsRestTemplate;
import com.xiaoxj.sqlworkflow.remote.request.DefaultHttpClientRequest;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.RequestContent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DolphinConfig {
    @Value("${dolphin.baseUrl}")
    private String dolphinAddress;
    @Value("${dolphin.authToken}")
    private String token;

    @Bean
    public RequestConfig requestConfig() {
        return RequestConfig.custom().build();
    }

    @Bean
    public CloseableHttpClient httpClient(RequestConfig requestConfig) {
        return HttpClients.custom()
                .addInterceptorLast(new RequestContent(true))
                .setDefaultRequestConfig(requestConfig)
                .build();
    }

    @Bean
    public DefaultHttpClientRequest defaultHttpClientRequest(CloseableHttpClient httpClient, RequestConfig requestConfig) {
        return new DefaultHttpClientRequest(httpClient, requestConfig);
    }

    @Bean
    public DolphinsRestTemplate dolphinsRestTemplate(DefaultHttpClientRequest defaultHttpClientRequest) {
        return new DolphinsRestTemplate(defaultHttpClientRequest);
    }

    @Bean
    public DolphinClient dolphinClient(DolphinsRestTemplate dolphinsRestTemplate) {
        return new DolphinClient(token, dolphinAddress, dolphinsRestTemplate);
    }
}
