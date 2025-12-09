package com.xiaoxj.sqlworkflow.dolphinscheduler.instance;

import lombok.Data;
import lombok.experimental.Accessors;

/** re run/recover workflow instance */
@Data
@Accessors(chain = true)
public class WorkflowInstanceRunParam {

  private Long workflowInstanceId;

  private String executeType;
}
