package com.xiaoxj.sqlworkflow.dolphinscheduler.datasource;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.xiaoxj.sqlworkflow.common.PageInfo;
import com.xiaoxj.sqlworkflow.core.AbstractOperator;
import com.xiaoxj.sqlworkflow.core.DolphinException;
import com.xiaoxj.sqlworkflow.remote.DolphinsRestTemplate;
import com.xiaoxj.sqlworkflow.remote.HttpRestResult;
import com.xiaoxj.sqlworkflow.remote.Query;
import com.xiaoxj.sqlworkflow.util.JacksonUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/** operator for operate datasource */
@Slf4j
public class DataSourceOperator extends AbstractOperator {

  public DataSourceOperator(
      String dolphinAddress, String token, DolphinsRestTemplate dolphinsRestTemplate) {
    super(dolphinAddress, token, dolphinsRestTemplate);
  }


  /**
   * page query dolphin datasource list ，api:/dolphinscheduler/datasources
   *
   * @return {@link List <DataSourceQueryResp>}
   */
  public List<DataSourceQueryResp> list(String dsName) {
    String url = dolphinAddress + "/datasources";
    Query query =
        new Query()
            .addParam("pageNo", "1")
            .addParam("pageSize", "10")
            .addParam("searchVal", dsName)
            .build();
    try {
      HttpRestResult<JsonNode> stringHttpRestResult =
          dolphinsRestTemplate.get(url, getHeader(), query, JsonNode.class);
      return JacksonUtils.parseObject(
              stringHttpRestResult.getData().toString(),
              new TypeReference<PageInfo<DataSourceQueryResp>>() {})
          .getTotalList();
    } catch (Exception e) {
      throw new DolphinException("list dolphin scheduler datasource fail", e);
    }
  }
}
