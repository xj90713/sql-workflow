package com.xiaoxj.sqlworkflow.dolphinscheduler.instance;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xiaoxj.sqlworkflow.common.result.PageInfo;
import com.xiaoxj.sqlworkflow.core.AbstractOperator;
import com.xiaoxj.sqlworkflow.core.DolphinClientConstant;
import com.xiaoxj.sqlworkflow.common.exception.DolphinException;
import com.xiaoxj.sqlworkflow.remote.DolphinsRestTemplate;
import com.xiaoxj.sqlworkflow.remote.HttpRestResult;
import com.xiaoxj.sqlworkflow.remote.Query;
import com.xiaoxj.sqlworkflow.common.utils.JacksonUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.stream.Collectors;

@Slf4j
public class WorkflowInstanceOperator extends AbstractOperator {

  public WorkflowInstanceOperator(
      String dolphinAddress, String token, DolphinsRestTemplate dolphinsRestTemplate) {
    super(dolphinAddress, token, dolphinsRestTemplate);
  }

  /**
   * start/run workflow instance
   *
   * <p>api: /dolphinscheduler/projects/{projectCode}/executors/start-workflow-instance
   *
   * @param workflowInstanceCreateParam workflow instance create param
   * @return true for success,otherwise false
   */
  public HttpRestResult<JsonNode>  start(Long projectCode, WorkflowInstanceCreateParam workflowInstanceCreateParam) {
    String url = dolphinAddress + "/projects/" + projectCode + "/executors/start-workflow-instance";
    log.info("start workflow instance ,url:{}", url);
    try {
      HttpRestResult<JsonNode> restResult =
          dolphinsRestTemplate.postForm(
              url, getHeader(), workflowInstanceCreateParam, JsonNode.class);
      Long workflowCode = workflowInstanceCreateParam.getWorkflowDefinitionCode();
      log.info("start workflow response:{}", restResult.getData().get(0).asLong());
      HashMap<String, Long> workflowMap = new HashMap<>();
      ObjectMapper mapper = new ObjectMapper();
      workflowMap.put("workflowCode", workflowCode);
      workflowMap.put("workflowInstanceId", restResult.getData().get(0).asLong());
      JsonNode jsonNode = mapper.convertValue(workflowMap, JsonNode.class);
      restResult.setData(jsonNode);
      return restResult;
    } catch (Exception e) {
      throw new DolphinException("start dolphin scheduler workflow instance fail", e);
    }
  }

  /**
   * patch start/run workflow instances
   *
   * <p>api: /dolphinscheduler/projects/{projectCode}/executors/batch-start-workflow-instance
   *
   * @param workflowInstanceCreateParams workflow instance create param
   * @return true for success,otherwise false
   */
  public Boolean batchStart(Long projectCode, WorkflowInstanceCreateParams workflowInstanceCreateParams) {
    String url = dolphinAddress + "/projects/" + projectCode + "/executors/batch-start-workflow-instance";
    log.info("batch start workflow instances ,url:{}", url);
    try {
      HttpRestResult<JsonNode> restResult =
              dolphinsRestTemplate.postForm(
                      url, getHeader(), workflowInstanceCreateParams, JsonNode.class);
      log.info("batch start workflows response:{}", restResult);
      return restResult.getSuccess();
    } catch (Exception e) {
      throw new DolphinException("batch start dolphin scheduler workflow instances fail", e);
    }
  }

  /**
   * page query workflow's instance list
   *
   * @param page page,default 1 while is null
   * @param size size,default 10 while is null
   * @param projectCode project code
   * @param workflowCode workflow id
   * @return
   */



  public String  getWorkflowInstanceIds(
          Integer page, Integer size, Long projectCode, Integer days) {

    // 1. 设置默认分页值
    page = Optional.ofNullable(page).orElse(DolphinClientConstant.Page.DEFAULT_PAGE);
    size = Optional.ofNullable(size).orElse(DolphinClientConstant.Page.DEFAULT_SIZE);
    // 如果未传入天数，默认 30 天
    int retentionDays = Optional.ofNullable(days).orElse(60);

    // 2. 动态计算日期范围
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    LocalDateTime now = LocalDateTime.now();
    // 起始时间：当前时间减去 X 天，并设置为当天的零点
    String startDate = "2025-01-01 00:00:00";
    // 结束时间：当前时间
    String endDate = now.minusDays(retentionDays).withHour(23).withMinute(59).withSecond(59).format(formatter);

    String url = dolphinAddress + "/projects/" + projectCode + "/workflow-instances";
    log.info("start time：{}，end time：{}", startDate, endDate);
    Query query = new Query();
    query
            .addParam("pageNo", String.valueOf(page))
            .addParam("pageSize", String.valueOf(size))
            .addParam("startDate", startDate) // 动态起始时间
            .addParam("endDate", endDate);     // 动态结束时间

    // 如果需要按具体的 workflowCode 过滤，通常也需要在此添加参数

    try {
      HttpRestResult<JsonNode> restResult =
              dolphinsRestTemplate.get(url, getHeader(), query, JsonNode.class);

      List<WorkflowInstanceQueryResp> totalList = JacksonUtils.parseObject(
                      restResult.getData().toString(),
                      new TypeReference<PageInfo<WorkflowInstanceQueryResp>>() {
                      })
              .getTotalList();
        return totalList.stream()
                .map(WorkflowInstanceQueryResp::getId)
                .map(String::valueOf)
                .collect(Collectors.joining(","));
    } catch (Exception e) {
      throw new DolphinException("require dolphin scheduler workflow instances ids fail", e);
    }
  }

      /**
   * query workflow's instance status
   *
   * @param projectCode project code
   * @param workflowInstanceId workflow id
   * @return
   */
  public String getWorkflowInstanceStatus(Long projectCode, Long workflowInstanceId) {
    String url = dolphinAddress + "/projects/" + projectCode + "/workflow-instances/" + workflowInstanceId;
    try {
      HttpRestResult<JsonNode> restResult =
              dolphinsRestTemplate.get(url, getHeader(), null, JsonNode.class);
      JsonNode state = restResult.getData().get("state");
      return state.textValue();
    } catch (Exception e) {
      throw new DolphinException("get workflow instance status fail", e);
    }
  }



  /**
   * repeat run dolphin scheduler workflow instance
   *
   * @param projectCode project code
   * @param workflowInstanceId workflow instance id
   * @return true for success,otherwise false
   */
  public Boolean reRun(Long projectCode, Long workflowInstanceId) {
    log.info("repeat run workflow instance,id:{}", workflowInstanceId);
    return execute(projectCode, workflowInstanceId, DolphinClientConstant.ExecuteType.RE_RUN);
  }

  /**
   * stop dolphin scheduler workflow instance
   *
   * @param projectCode project code
   * @param workflowInstanceId workflow instance id
   * @return true for success,otherwise false
   */
  public Boolean stop(Long projectCode, Long workflowInstanceId) {
    log.info("stop workflow instance,id:{}", workflowInstanceId);
    return execute(projectCode, workflowInstanceId, DolphinClientConstant.ExecuteType.STOP);
  }

  /**
   * pause dolphin scheduler workflow instance
   *
   * @param projectCode project code
   * @param workflowInstanceId workflow instance id
   * @return true for success,otherwise false
   */
  public Boolean pause(Long projectCode, Long workflowInstanceId) {
    log.info("stop workflow instance,id:{}", workflowInstanceId);
    return execute(projectCode, workflowInstanceId, DolphinClientConstant.ExecuteType.PAUSE);
  }

  /**
   * execute dolphin scheduler workflow instance with custom execute type
   *
   * @param projectCode project code
   * @param workflowInstanceId workflow instance id
   * @return true for success,otherwise false
   */
  public Boolean execute(Long projectCode, Long workflowInstanceId, String executeType) {
    String url = dolphinAddress + "/projects/" + projectCode + "/executors/execute";
    WorkflowInstanceRunParam reWorkflowInstanceRunParam =
        new WorkflowInstanceRunParam()
            .setWorkflowInstanceId(workflowInstanceId)
            .setExecuteType(executeType);
    try {
      HttpRestResult<String> restResult =
          dolphinsRestTemplate.postForm(url, getHeader(), reWorkflowInstanceRunParam, String.class);
      return restResult.getSuccess();
    } catch (Exception e) {
      throw new DolphinException(executeType + " dolphin scheduler workflow instance fail", e);
    }
  }

  public Boolean delete(Long projectCode, Long workflowInstanceId) {
    String url =
        dolphinAddress + "/projects/" + projectCode + "/workflow-instances/" + workflowInstanceId;
    try {
      HttpRestResult<String> restResult =
          dolphinsRestTemplate.delete(url, getHeader(), null, String.class);
      return restResult.getSuccess();
    } catch (Exception e) {
      throw new DolphinException("delete dolphin scheduler workflow instance fail", e);
    }
  }

  public Boolean batchDelete(Long projectCode, String workflowInstanceIds) {
    // 构建包含查询参数的URL
    String url = dolphinAddress + "/projects/" + projectCode + "/workflow-instances/batch-delete?workflowInstanceIds=" + workflowInstanceIds;

    try {
      // 发送POST请求，请求体为空。这里假设postForm方法在第二个参数为null或空时发送空体。
      HttpRestResult<JsonNode> restResult = dolphinsRestTemplate.postForm(url, getHeader(), null, JsonNode.class);
      return restResult.getSuccess();
    } catch (Exception e) {
      throw new DolphinException("delete dolphin scheduler workflow instances fail", e);
    }
  }
}
