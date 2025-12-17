package com.xiaoxj.sqlworkflow.dolphinscheduler.workflow;

import com.xiaoxj.sqlworkflow.dolphinscheduler.task.TaskDefinition;
import com.xiaoxj.sqlworkflow.dolphinscheduler.task.TaskLocation;
import com.xiaoxj.sqlworkflow.dolphinscheduler.task.TaskRelation;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.List;

/**
 * dolphin scheduler define workflow param
 *
 * <p>dolphin scheduler use post form type to create workflow,so in fact every attribute is string
 * type
 *
 * <p>but in order to develop easier,we use TaskLocation,TaskDefinition,TaskRelation object,and
 * rewrite toString method
 *
 * <p>so that,when begin to send request,we will transfer this object to json string
 */
@Data
@Accessors(chain = true)
public class WorkflowDefineParam {

  public static final String EXECUTION_TYPE_PARALLEL = "PARALLEL";
  public static final String EXECUTION_TYPE_SERIAL_WAIT = "SERIAL_WAIT";
  public static final String EXECUTION_TYPE_SERIAL_DISCARD = "SERIAL_DISCARD";
  public static final String EXECUTION_TYPE_SERIAL_PRIORITY = "SERIAL_PRIORITY";

  /** workflow name */
  private String name;

  /** location */
  private List<TaskLocation> locations;

  private List<TaskDefinition> taskDefinitionJson;

  private List<TaskRelation> taskRelationJson;

  /** tenant code */
  private String tenantCode;

  /** desc for workflow */
  private String description;

  /**
   * PARALLEL,SERIAL_WAIT,SERIAL_DISCARD,SERIAL_PRIORITY
   *
   * <p>@see org.apache.dolphinscheduler.common.enums.workflowExecutionTypeEnum
   */
  private String executionType;

  /** global params */
  private List<Parameter> globalParams;

  private String timeout;

  public static WorkflowDefineParam newParallelInstance() {
    return new WorkflowDefineParam().setExecutionType(EXECUTION_TYPE_PARALLEL);
  }

  public static WorkflowDefineParam newSerialWaitInstance() {
    return new WorkflowDefineParam().setExecutionType(EXECUTION_TYPE_SERIAL_WAIT);
  }

  public static WorkflowDefineParam newSerialDiscardInstance() {
    return new WorkflowDefineParam().setExecutionType(EXECUTION_TYPE_SERIAL_DISCARD);
  }

  public static WorkflowDefineParam newSerialPriorityInstance() {
    return new WorkflowDefineParam().setExecutionType(EXECUTION_TYPE_SERIAL_PRIORITY);
  }
}
