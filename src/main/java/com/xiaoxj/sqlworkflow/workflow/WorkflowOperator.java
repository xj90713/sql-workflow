package com.xiaoxj.sqlworkflow.workflow;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.xiaoxj.sqlworkflow.common.PageInfo;
import com.xiaoxj.sqlworkflow.core.AbstractOperator;
import com.xiaoxj.sqlworkflow.core.DolphinClientConstant;
import com.xiaoxj.sqlworkflow.core.DolphinException;
import com.xiaoxj.sqlworkflow.remote.DolphinsRestTemplate;
import com.xiaoxj.sqlworkflow.remote.HttpRestResult;
import com.xiaoxj.sqlworkflow.remote.Query;
import com.xiaoxj.sqlworkflow.util.JacksonUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;

@Slf4j
public class WorkflowOperator extends AbstractOperator {

  public WorkflowOperator(
      String dolphinAddress, String token, DolphinsRestTemplate dolphinsRestTemplate) {
    super(dolphinAddress, token, dolphinsRestTemplate);
  }

  /**
   * page query process define(workflow)
   *
   * @param projectCode project code
   * @param page page
   * @param size size
   * @param searchVal process name
   * @return list
   */
  public List<WrokflowDefineResp> page(
      Long projectCode, Integer page, Integer size, String searchVal) {
    page = Optional.ofNullable(page).orElse(DolphinClientConstant.Page.DEFAULT_PAGE);
    size = Optional.ofNullable(size).orElse(DolphinClientConstant.Page.DEFAULT_SIZE);
    searchVal = Optional.ofNullable(searchVal).orElse("");

    String url = dolphinAddress + "/projects/" + projectCode + "/workflow-definition";
    Query query =
        new Query()
            .addParam("pageNo", String.valueOf(page))
            .addParam("pageSize", String.valueOf(size))
            .addParam("searchVal", searchVal);

    try {
      HttpRestResult<JsonNode> restResult =
          dolphinsRestTemplate.get(url, getHeader(), query, JsonNode.class);

      return JacksonUtils.parseObject(
              restResult.getData().toString(), new TypeReference<PageInfo<WrokflowDefineResp>>() {})
          .getTotalList();
    } catch (Exception e) {
      throw new DolphinException("list dolphin scheduler workflow fail", e);
    }
  }

  /**
   * create dolphin scheduler process api:
   * /dolphinscheduler/projects/{projectCode}/workflow-definition
   *
   * @param projectCode project code
   * @param wrokflowDefineParam create process param
   * @return create response
   */
  public WrokflowDefineResp create(Long projectCode, WrokflowDefineParam wrokflowDefineParam) {
    String url = dolphinAddress + "/projects/" + projectCode + "/workflow-definition";
    log.info(
        "create process definition, url:{}, param:{}",
        url,
        JacksonUtils.toJSONString(wrokflowDefineParam));
    try {
      HttpRestResult<WrokflowDefineResp> restResult =
          dolphinsRestTemplate.postForm(
              url, getHeader(), wrokflowDefineParam, WrokflowDefineResp.class);
      if (restResult.getSuccess()) {
        return restResult.getData();
      } else {
        log.error("dolphin scheduler response:{}", restResult);
        throw new DolphinException("create dolphin scheduler workflow fail");
      }
    } catch (Exception e) {
      throw new DolphinException("create dolphin scheduler workflow fail", e);
    }
  }

  /**
   * update dolphin scheduler workflow
   *
   * <p>api:/dolphinscheduler/projects/{projectCode}/workflow-definition/{workflow-definition-code}
   *
   * @param wrokflowDefineParam update process def param
   * @param processCode workflow code
   * @return update response json
   */
  public WrokflowDefineResp update(
          Long projectCode, WrokflowDefineParam wrokflowDefineParam, Long processCode) {
    String url = dolphinAddress + "/projects/" + projectCode + "/workflow-definition/" + processCode;
    log.info("update process definition, url:{}, param:{}", url, wrokflowDefineParam);
    try {
      HttpRestResult<WrokflowDefineResp> restResult =
          dolphinsRestTemplate.putForm(
              url, getHeader(), wrokflowDefineParam, WrokflowDefineResp.class);
      if (restResult.getSuccess()) {
        return restResult.getData();
      } else {
        log.error("dolphin scheduler response:{}", restResult);
        throw new DolphinException("update dolphin scheduler workflow fail");
      }
    } catch (Exception e) {
      throw new DolphinException("update dolphin scheduler workflow fail", e);
    }
  }

  /**
   * delete process
   *
   * @param projectCode project code
   * @param processCode process code
   * @return true for success,otherwise false
   */
  public Boolean delete(Long projectCode, Long processCode) {
    String url = dolphinAddress + "/projects/" + projectCode + "/workflow-definition/" + processCode;
    log.info("delete process definition,processCode:{}, url:{}", processCode, url);
    try {
      HttpRestResult<String> restResult =
          dolphinsRestTemplate.delete(url, getHeader(), null, String.class);
      return restResult.getSuccess();
    } catch (Exception e) {
      throw new DolphinException("delete dolphin scheduler workflow fail", e);
    }
  }

  /**
   * release, api: /dolphinscheduler/projects/{projectCode}/workflow-definition/{code}/release
   *
   * @param projectCode project code
   * @param code workflow id
   * @param workflowReleaseParam param
   * @return true for success,otherwise false
   */
  public Boolean release(Long projectCode, Long code, WorkflowReleaseParam workflowReleaseParam) {
    String url =
        dolphinAddress + "/projects/" + projectCode + "/workflow-definition/" + code + "/release";
    log.info("release process definition,url:{}, param:{}", url, workflowReleaseParam);
    try {
      HttpRestResult<String> restResult =
          dolphinsRestTemplate.postForm(url, getHeader(), workflowReleaseParam, String.class);
      return restResult.getSuccess();
    } catch (Exception e) {
      throw new DolphinException("release dolphin scheduler workflow fail", e);
    }
  }

  /**
   * online workflow, this method can replace {@link #release(Long, Long, WorkflowReleaseParam)}
   *
   * @param projectCode project code
   * @param code workflow id
   * @return true for success,otherwise false
   */
  public Boolean online(Long projectCode, Long code) {
    return release(projectCode, code, WorkflowReleaseParam.newOnlineInstance());
  }

  /**
   * offline workflow, this method can replace {@link #release(Long, Long, WorkflowReleaseParam)}
   *
   * @param projectCode project code
   * @param code workflow id
   * @return true for success,otherwise false
   */
  public Boolean offline(Long projectCode, Long code) {
    return release(projectCode, code, WorkflowReleaseParam.newOfflineInstance());
  }

  /**
   * generate task code
   *
   * @param projectCode project's code
   * @param codeNumber the number of task code
   * @return task code list
   */
  public List<Long> generateTaskCode(Long projectCode, int codeNumber) {
    String url = dolphinAddress + "/projects/" + projectCode + "/task-definition/gen-task-codes";
    Query query = new Query();
    query.addParam("genNum", String.valueOf(codeNumber));
    try {
      HttpRestResult<List> restResult =
          dolphinsRestTemplate.get(url, getHeader(), query, List.class);
      return (List<Long>) restResult.getData();
    } catch (Exception e) {
      throw new DolphinException("generate task code fail", e);
    }
  }
}
