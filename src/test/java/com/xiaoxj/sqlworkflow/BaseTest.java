package com.xiaoxj.sqlworkflow;


import com.xiaoxj.sqlworkflow.core.DolphinClient;
import com.xiaoxj.sqlworkflow.remote.DolphinsRestTemplate;
import com.xiaoxj.sqlworkflow.remote.request.DefaultHttpClientRequest;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.RequestContent;

public class BaseTest {

  protected final String dolphinAddress = "https://ds.gp51.com/dolphinscheduler";
  protected final Long projectCode = 158327516778560L;
  private final String token = "28459a89b01e79187adeb206334d0abd";
  protected final String tenantCode = "admin";

  protected DolphinsRestTemplate restTemplate =
      new DolphinsRestTemplate(
          new DefaultHttpClientRequest(
              HttpClients.custom()
                  .addInterceptorLast(new RequestContent(true))
                  .setDefaultRequestConfig(RequestConfig.custom().build())
                  .build(),
              RequestConfig.custom().build()));

  protected DolphinClient getClient() {
    return new DolphinClient(token, dolphinAddress, restTemplate);
  }
}
