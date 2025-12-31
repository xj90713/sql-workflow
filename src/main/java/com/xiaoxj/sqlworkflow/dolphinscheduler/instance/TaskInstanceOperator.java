package com.xiaoxj.sqlworkflow.dolphinscheduler.instance;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.xiaoxj.sqlworkflow.common.result.PageInfo;
import com.xiaoxj.sqlworkflow.core.AbstractOperator;
import com.xiaoxj.sqlworkflow.core.DolphinClientConstant;
import com.xiaoxj.sqlworkflow.core.DolphinException;
import com.xiaoxj.sqlworkflow.remote.DolphinsRestTemplate;
import com.xiaoxj.sqlworkflow.remote.HttpRestResult;
import com.xiaoxj.sqlworkflow.remote.Query;
import com.xiaoxj.sqlworkflow.common.utils.JacksonUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;

@Slf4j
public class TaskInstanceOperator extends AbstractOperator {

  public TaskInstanceOperator(
      String dolphinAddress, String token, DolphinsRestTemplate dolphinsRestTemplate) {
    super(dolphinAddress, token, dolphinsRestTemplate);
  }

  /**
   * page query task instance
   *
   * @param projectCode project code
   * @param page page
   * @param size size
   * @param workflowInstanceId workflow instance id
   * @return list
   */
  public List<TaskInstanceQueryResp> page(
      Long projectCode, Integer page, Integer size, Long workflowInstanceId) {
    page = Optional.ofNullable(page).orElse(DolphinClientConstant.Page.DEFAULT_PAGE);
    size = Optional.ofNullable(size).orElse(DolphinClientConstant.Page.DEFAULT_SIZE);

    String url = dolphinAddress + "/projects/" + projectCode + "/task-instances";
    Query query =
        new Query()
            .addParam("pageNo", String.valueOf(page))
            .addParam("pageSize", String.valueOf(size))
            .addParam("workflowInstanceId", String.valueOf(workflowInstanceId));

    try {
      HttpRestResult<JsonNode> restResult =
          dolphinsRestTemplate.get(url, getHeader(), query, JsonNode.class);

      return JacksonUtils.parseObject(
              restResult.getData().toString(),
              new TypeReference<PageInfo<TaskInstanceQueryResp>>() {})
          .getTotalList();
    } catch (Exception e) {
      throw new DolphinException("list ds task instance fail", e);
    }
  }

  /**
   * query task instance log
   *
   * @param projectCode project code
   * @param skipLineNum skipLineNum
   * @param limit limit
   * @param taskInstanceId taskInstanceId
   * @return String
   */
  public String queryLog(
      Long projectCode, Integer skipLineNum, Integer limit, Long taskInstanceId) {
    skipLineNum =
        Optional.ofNullable(skipLineNum).orElse(DolphinClientConstant.LogLimit.DEFAULT_SKIP);
    limit = Optional.ofNullable(limit).orElse(DolphinClientConstant.LogLimit.DEFAULT_LIMIT);

    String url = dolphinAddress + "/log/" + projectCode + "/detail";
    Query query =
        new Query()
            .addParam("projectCode", String.valueOf(projectCode))
            .addParam("taskInstanceId", String.valueOf(taskInstanceId))
            .addParam("skipLineNum", String.valueOf(skipLineNum))
            .addParam("limit", String.valueOf(limit));

    try {
      HttpRestResult<JsonNode> restResult =
          dolphinsRestTemplate.get(url, getHeader(), query, JsonNode.class);

      return restResult.getData().toString();
    } catch (Exception e) {
      throw new DolphinException("query ds log detail fail", e);
    }
  }
}
