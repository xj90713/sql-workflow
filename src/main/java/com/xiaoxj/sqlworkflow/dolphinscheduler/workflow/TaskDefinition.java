package com.xiaoxj.sqlworkflow.dolphinscheduler.workflow;


import com.xiaoxj.sqlworkflow.dolphinscheduler.task.AbstractTask;
import com.xiaoxj.sqlworkflow.util.JacksonUtils;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class TaskDefinition {

  private Long code;

  private Integer version;

  /** the task node's name */
  private String name;

  /** the task node's description */
  private String description;

  private String taskType;

  private AbstractTask taskParams;

  /** NO:the node will not execute;YES:the node will execute,default is YES */
  private String flag;

  private String taskPriority;

  private String workerGroup;

  private String failRetryTimes;

  private String failRetryInterval;

  private String timeoutFlag;

  private String timeoutNotifyStrategy;

  private Integer timeout = 0;

  private String delayTime = "0";

  private Integer environmentCode = -1;

  private String taskExecuteType;

  private Integer cpuQuota = -1;

  private Long memoryMax = -1L;

  /** YES, NO * */
  private String isCache = "NO";

  /**
   *
   * @return object json string
   */
  @Override
  public String toString() {
    return JacksonUtils.toJSONString(this);
  }
}
