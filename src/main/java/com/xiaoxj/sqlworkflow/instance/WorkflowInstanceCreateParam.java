package com.xiaoxj.sqlworkflow.instance;

import lombok.Data;
import lombok.experimental.Accessors;

/** workflow instance create param */
@Data
@Accessors(chain = true)
public class WorkflowInstanceCreateParam {

  /** continue or and */
  private String failureStrategy;

  private Long workflowDefinitionCode;

  private String workflowInstancePriority;

  private String scheduleTime;

  private Long warningGroupId;

  private String warningType;

  /** o or 1 */
  private Integer dryRun;

  /** env code */
  private String environmentCode;

  private String execType;

  private String expectedParallelismNumber;

  /** run mode,value:RUN_MODE_SERIAL,RUN_MODE_PARALLEL */
  private String runMode;

  private String startNodeList;

  private String startParams;

  private String taskDependType;

  /** worker group */
  private String workerGroup;
}
