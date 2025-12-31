package com.xiaoxj.sqlworkflow.dolphinscheduler.task;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.xiaoxj.sqlworkflow.dolphinscheduler.workflow.Parameter;
import com.xiaoxj.sqlworkflow.common.utils.JacksonUtils;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@Data
@Accessors(chain = true)
public abstract class AbstractTask {

  protected ObjectNode dependence = JacksonUtils.createObjectNode();
  protected ObjectNode conditionResult;
  protected ObjectNode waitStartTimeout = JacksonUtils.createObjectNode();
  protected ObjectNode switchResult = JacksonUtils.createObjectNode();

  /** local params */
  protected List<Parameter> localParams = Collections.emptyList();

  protected List<Parameter> taskParamList = Collections.emptyList();

  protected Map<String, String> taskParamMap = Collections.emptyMap();

  public abstract String getTaskType();
}
