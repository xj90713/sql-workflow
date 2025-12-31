package com.xiaoxj.sqlworkflow.dolphinscheduler.schedule;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.xiaoxj.sqlworkflow.common.result.PageInfo;
import com.xiaoxj.sqlworkflow.core.AbstractOperator;
import com.xiaoxj.sqlworkflow.core.DolphinException;
import com.xiaoxj.sqlworkflow.remote.DolphinsRestTemplate;
import com.xiaoxj.sqlworkflow.remote.HttpRestResult;
import com.xiaoxj.sqlworkflow.remote.Query;
import com.xiaoxj.sqlworkflow.common.utils.JacksonUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class ScheduleOperator extends AbstractOperator {

  public ScheduleOperator(
      String dolphinAddress, String token, DolphinsRestTemplate dolphinsRestTemplate) {
    super(dolphinAddress, token, dolphinsRestTemplate);
  }

  /**
   * create schedule
   *
   * @param projectCode project code
   * @param scheduleDefineParam define param
   * @return {@link ScheduleInfoResp}
   */
  public ScheduleInfoResp create(Long projectCode, ScheduleDefineParam scheduleDefineParam) {
    String url = dolphinAddress + "/projects/" + projectCode + "/schedules";
    log.info("create schedule, url:{}, defineParam:{}", url, scheduleDefineParam);
    try {
      HttpRestResult<ScheduleInfoResp> restResult =
          dolphinsRestTemplate.postForm(
              url, getHeader(), scheduleDefineParam, ScheduleInfoResp.class);
      if (restResult.getSuccess()) {
        return restResult.getData();
      } else {
        log.error("dolphin scheduler response:{}", restResult);
        throw new DolphinException("create dolphin scheduler schedule fail");
      }
    } catch (Exception e) {
      throw new DolphinException("create dolphin scheduler schedule fail", e);
    }
  }

  /**
   * get schedule by workflow
   *
   * @param projectCode project's code
   * @param workflowDefinitionCode workflow code
   * @return {@link List<ScheduleInfoResp>}
   */
  public List<ScheduleInfoResp> getScheduleByWorkflowCode(Long projectCode, Long workflowDefinitionCode) {
    String url = dolphinAddress + "/projects/" + projectCode + "/schedules";
    Query query =
        new Query()
            .addParam("pageNo", "1")
            .addParam("pageSize", "10")
            .addParam("workflowDefinitionCode", String.valueOf(workflowDefinitionCode));
    try {
      HttpRestResult<JsonNode> stringHttpRestResult =
          dolphinsRestTemplate.get(url, getHeader(), query, JsonNode.class);
      return JacksonUtils.parseObject(
              stringHttpRestResult.getData().toString(),
              new TypeReference<PageInfo<ScheduleInfoResp>>() {})
          .getTotalList();
    } catch (Exception e) {
      throw new DolphinException("list dolphin scheduler schedule fail", e);
    }
  }

  /**
   * update schedule
   *
   * @param projectCode project code
   * @param scheduleDefineParam define param
   * @return {@link ScheduleInfoResp}
   */
  public ScheduleInfoResp update(
      Long projectCode, Long scheduleId, ScheduleDefineParam scheduleDefineParam) {
    String url = dolphinAddress + "/projects/" + projectCode + "/schedules/" + scheduleId;
    log.info("update schedule, url:{}, defineParam:{}", url, scheduleDefineParam);
    try {
      HttpRestResult<ScheduleInfoResp> restResult =
          dolphinsRestTemplate.putForm(
              url, getHeader(), scheduleDefineParam, ScheduleInfoResp.class);
      if (restResult.getSuccess()) {
        return restResult.getData();
      } else {
        log.error("dolphin scheduler response:{}", restResult);
        throw new DolphinException("update dolphin scheduler schedule fail");
      }
    } catch (Exception e) {
      throw new DolphinException("update dolphin scheduler schedule fail", e);
    }
  }

  /**
   * online schedule
   *
   * @param projectCode project code
   * @param scheduleId id
   * @return true for success,otherwise false
   */
  public Boolean online(Long projectCode, Long scheduleId) {
    String url =
        dolphinAddress + "/projects/" + projectCode + "/schedules/" + scheduleId + "/online";
    log.info("online schedule, scheduleId:{}, url:{}", scheduleId, url);
    try {
      HttpRestResult<String> restResult =
          dolphinsRestTemplate.postForm(url, getHeader(), null, String.class);
      return restResult.getSuccess();
    } catch (Exception e) {
      throw new DolphinException("online dolphin scheduler schedule fail", e);
    }
  }

  /**
   * offline schedule
   *
   * @param projectCode project code
   * @param scheduleId id
   * @return true for success,otherwise false
   */
  public Boolean offline(Long projectCode, Long scheduleId) {
    String url =
        dolphinAddress + "/projects/" + projectCode + "/schedules/" + scheduleId + "/offline";
    log.info("offline schedule, scheduleId:{}, url:{}", scheduleId, url);
    try {
      HttpRestResult<String> restResult =
          dolphinsRestTemplate.postForm(url, getHeader(), null, String.class);
      return restResult.getSuccess();
    } catch (Exception e) {
      throw new DolphinException("offline dolphin scheduler schedule fail", e);
    }
  }

  /**
   * delete schedule
   *
   * @param projectCode project code
   * @param scheduleId id
   * @return true for success,otherwise false
   */
  public Boolean delete(Long projectCode, Long scheduleId) {
    String url = dolphinAddress + "/projects/" + projectCode + "/schedules/" + scheduleId;
    log.info("offline schedule, scheduleId:{}, url:{}", scheduleId, url);
    try {
      HttpRestResult<String> restResult =
          dolphinsRestTemplate.delete(url, getHeader(), null, String.class);
      return restResult.getSuccess();
    } catch (Exception e) {
      throw new DolphinException("delete dolphin scheduler schedule fail", e);
    }
  }
}
