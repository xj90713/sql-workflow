package com.xiaoxj.sqlworkflow.dolphinscheduler.workflow;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.xiaoxj.sqlworkflow.common.result.PageInfo;
import com.xiaoxj.sqlworkflow.core.AbstractOperator;
import com.xiaoxj.sqlworkflow.core.DolphinClientConstant;
import com.xiaoxj.sqlworkflow.common.exception.DolphinException;
import com.xiaoxj.sqlworkflow.remote.DolphinsRestTemplate;
import com.xiaoxj.sqlworkflow.remote.HttpRestResult;
import com.xiaoxj.sqlworkflow.remote.Query;
import com.xiaoxj.sqlworkflow.common.utils.JacksonUtils;
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
   * page query workflow define(workflow)
   *
   * @param projectCode project code
   * @param page page
   * @param size size
   * @param searchVal workflow name
   * @return list
   */
  public List<WorkflowDefineResp> page(
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
              restResult.getData().toString(), new TypeReference<PageInfo<WorkflowDefineResp>>() {})
          .getTotalList();
    } catch (Exception e) {
      throw new DolphinException("list dolphin scheduler workflow fail", e);
    }
  }

  /**
   * create dolphin scheduler workflow api:
   * /dolphinscheduler/projects/{projectCode}/workflow-definition
   *
   * @param projectCode project code
   * @param workflowDefineParam create workflow param
   * @return create response
   */
  public WorkflowDefineResp create(Long projectCode, WorkflowDefineParam workflowDefineParam) {
    String url = dolphinAddress + "/projects/" + projectCode + "/workflow-definition";
    log.info(
        "create workflow definition, url:{}, param:{}",
        url,
        JacksonUtils.toJSONString(workflowDefineParam));
    try {
      HttpRestResult<WorkflowDefineResp> restResult =
          dolphinsRestTemplate.postForm(
              url, getHeader(), workflowDefineParam, WorkflowDefineResp.class);
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
   * @param workflowDefineParam update workflow def param
   * @param workflowCode workflow code
   * @return update response json
   */
  public WorkflowDefineResp update(
          Long projectCode, WorkflowDefineParam workflowDefineParam, Long workflowCode) {
    String url = dolphinAddress + "/projects/" + projectCode + "/workflow-definition/" + workflowCode;
    log.info("update workflow definition, url:{}, param:{}", url, workflowDefineParam);
    try {
      HttpRestResult<WorkflowDefineResp> restResult =
          dolphinsRestTemplate.putForm(
              url, getHeader(), workflowDefineParam, WorkflowDefineResp.class);
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
   * get dolphin scheduler workflow
   *
   * <p>api:/dolphinscheduler/projects/{projectCode}/workflow-definition/{workflow-definition-code}
   *
   * @param workflowDefineParam get workflow
   * @param workflowCode workflow code
   * @return get response json
   */
  public WorkflowDefineResp getWorkflow(
          Long projectCode, String workflowName) {
    String url = dolphinAddress + "/projects/" + projectCode + "/workflow-definition/query-by-name";
    Query query = new Query();
    query.addParam("name", workflowName);
    log.info("get workflow definition, url:{}, param:{}", url);
    try {
      HttpRestResult<JsonNode> restResult = dolphinsRestTemplate.get(
              url, getHeader(), query, JsonNode.class);
      if (restResult.getSuccess()) {
         return JacksonUtils.parseObject(
                restResult.getData().get("workflowDefinition").toString(), WorkflowDefineResp.class);
      } else {
        log.error("dolphin scheduler response:{}", restResult);
        throw new DolphinException("get dolphin scheduler workflow fail");
      }
    } catch (Exception e) {
      throw new DolphinException("get dolphin scheduler workflow fail", e);
    }
  }

  /**
   * delete workflow
   *
   * @param projectCode project code
   * @param workflowCode workflow code
   * @return true for success,otherwise false
   */
  public Boolean delete(Long projectCode, Long workflowCode) {
    String url = dolphinAddress + "/projects/" + projectCode + "/workflow-definition/" + workflowCode;
    log.info("delete workflow definition,workflowCode:{}, url:{}", workflowCode, url);
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
    log.info("release workflow definition,url:{}, param:{}", url, workflowReleaseParam);
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
    log.info("generate task code, url:{}, param:{}", url, codeNumber);
    Query query = new Query();
    query.addParam("genNum", String.valueOf(codeNumber));
    try {
      HttpRestResult<List> restResult =
          dolphinsRestTemplate.get(url, getHeader(), query, List.class);
      log.info("generate task code response:{}", restResult);
      return (List<Long>) restResult.getData();
    } catch (Exception e) {
      throw new DolphinException("generate task code fail", e);
    }
  }


}
