package com.xiaoxj.sqlworkflow.dolphinscheduler.instance;

import lombok.Data;
import lombok.experimental.Accessors;

/**  create muti params for batch submit workflow */
@Data
@Accessors(chain = true)
public class WorkflowInstanceCreateParams {

  /** continue or and */
  private String failureStrategy;

  private String workflowDefinitionCodes;

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

  /** tenant code */
  private String tenantCode;

  private String complementDependentMode;

  private String allLevelDependent;

  private String executionOrder;
}
