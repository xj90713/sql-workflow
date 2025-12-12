package com.xiaoxj.sqlworkflow.dolphinscheduler.task;

import com.xiaoxj.sqlworkflow.dolphinscheduler.workflow.Parameter;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import java.util.Collections;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
@Accessors(chain = true)
public class ProcedureTask extends AbstractTask {

  /** datasource type */
  private String type;

  /** datasource id */
  private Integer datasource;

  private String method;

  /** resource list */
  private List<TaskResource> resourceList = Collections.emptyList();

  private List<Parameter> localParams = Collections.emptyList();

  @Override
  public String getTaskType() {
    return "PROCEDURE";
  }
}
