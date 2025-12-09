package com.xiaoxj.sqlworkflow.util;


import com.google.common.base.Strings;
import com.xiaoxj.sqlworkflow.dolphinscheduler.task.HivecliTask;
import com.xiaoxj.sqlworkflow.dolphinscheduler.task.HttpTask;
import com.xiaoxj.sqlworkflow.dolphinscheduler.task.ShellTask;
import com.xiaoxj.sqlworkflow.dolphinscheduler.workflow.TaskDefinition;
import com.xiaoxj.sqlworkflow.dolphinscheduler.task.AbstractTask;
import com.xiaoxj.sqlworkflow.dolphinscheduler.workflow.WrokflowDefineParam;
import com.xiaoxj.sqlworkflow.dolphinscheduler.workflow.WrokflowDefineResp;
import com.xiaoxj.sqlworkflow.enums.HttpCheckCondition;
import com.xiaoxj.sqlworkflow.remote.HttpMethod;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TaskDefinitionUtils {

  public static final String FLAG_YES = "YES"; // the node will be executed
  public static final String FLAG_NO = "NO"; // the node will not be executed

  public static final String PRIORITY_HIGHEST = "HIGHEST";
  public static final String PRIORITY_HIGH = "HIGH";
  public static final String PRIORITY_MEDIUM = "MEDIUM";
  public static final String PRIORITY_LOW = "LOW";
  public static final String PRIORITY_LOWEST = "LOWEST";

  public static final String IS_CACHE_YES = "YES"; // execution cache
  public static final String IS_CACHE_NO = "NO"; // execution not cache

  /**
   * create task definition with default config {@link TaskDefinition} which can satisfy basic needs
   *
   * @param taskCode task node's code,generate by api
   * @param task {@link AbstractTask}
   */
  public static TaskDefinition createDefaultTaskDefinition(Long taskCode, AbstractTask task) {
    return createTaskDefinition(null, taskCode, 0, task, FLAG_YES, PRIORITY_MEDIUM, null, null);
  }

  public static TaskDefinition createDefaultTaskDefinition(Long taskCode, AbstractTask task, String description) {
    return createTaskDefinition(null, taskCode, 0, task, FLAG_YES, PRIORITY_MEDIUM, null, null, description);
  }

  /**
   * create task definition with default config {@link TaskDefinition} which can satisfy basic needs
   *
   * @param taskName task node's name
   * @param taskCode task node's code,generate by api
   * @param task {@link AbstractTask}
   */
  public static TaskDefinition createDefaultTaskDefinition(
      String taskName, Long taskCode, AbstractTask task) {
    return createTaskDefinition(taskName, taskCode, 0, task, FLAG_YES, PRIORITY_MEDIUM, null, null);
  }

  /**
   * create task definition with default config {@link TaskDefinition} which can satisfy basic needs
   *
   * @param taskCode task node's code,generate by api
   * @param version task node's version
   * @param task {@link AbstractTask}
   */
  public static TaskDefinition createDefaultTaskDefinition(
      Long taskCode, Integer version, AbstractTask task) {
    return createTaskDefinition(
        null, taskCode, version, task, FLAG_YES, PRIORITY_MEDIUM, null, null);
  }

  /**
   * create task which wll not be executed
   *
   * @param taskCode task node's code,generate by api
   * @param task {@link AbstractTask}
   */
  public static TaskDefinition createBannedTaskDefinition(
      Long taskCode, Integer version, AbstractTask task) {
    return createTaskDefinition(
        null, taskCode, version, task, FLAG_NO, PRIORITY_MEDIUM, null, null);
  }

  /**
   * create high priority task
   *
   * @param taskCode task node's code,generate by api
   * @param task {@link AbstractTask}
   */
  public static TaskDefinition createHighLevelTaskDefinition(
      Long taskCode, Integer version, AbstractTask task) {
    return createTaskDefinition(null, taskCode, version, task, FLAG_YES, PRIORITY_HIGH, null, null);
  }

  /**
   * create task definition
   *
   * @param taskName task node's name
   * @param taskCode task node's code,generate by api
   * @param version task node's version
   * @param task {@link AbstractTask}
   * @param flag YES or NO
   * @param cpuQuota cpu resource
   * @param memoryMax memory resource
   * @param taskPriority {@link #PRIORITY_HIGH,#PRIORITY_HIGHEST}...
   */
  public static TaskDefinition createTaskDefinition(
      String taskName,
      Long taskCode,
      Integer version,
      AbstractTask task,
      String flag,
      String taskPriority,
      Integer cpuQuota,
      Long memoryMax) {
    TaskDefinition taskDefinition = new TaskDefinition();
    if (Strings.isNullOrEmpty(taskName)) {
      taskName = task.getTaskType().concat(String.valueOf(System.currentTimeMillis()));
    }

    taskDefinition
        .setCode(taskCode)
        .setVersion(version)
        .setName(taskName)
        .setDescription("")
        .setTaskType(task.getTaskType())
        .setTaskParams(task)
        .setFlag(flag)
        .setTaskPriority(taskPriority)
        .setWorkerGroup("default")
        .setFailRetryTimes("0")
        .setFailRetryInterval("1")
        .setTimeoutFlag("CLOSE")
        .setTimeoutNotifyStrategy("WARN")
        .setIsCache("NO");
    Optional.ofNullable(cpuQuota).ifPresent(taskDefinition::setCpuQuota);
    Optional.ofNullable(memoryMax).ifPresent(taskDefinition::setMemoryMax);
    return taskDefinition;
  }

  public static TaskDefinition createTaskDefinition(
          String taskName,
          Long taskCode,
          Integer version,
          AbstractTask task,
          String flag,
          String taskPriority,
          Integer cpuQuota,
          Long memoryMax,
          String description) {
    TaskDefinition taskDefinition = new TaskDefinition();
    if (Strings.isNullOrEmpty(taskName)) {
      taskName = task.getTaskType().concat(String.valueOf(System.currentTimeMillis()));
    }

    taskDefinition
            .setCode(taskCode)
            .setVersion(version)
            .setName(taskName)
            .setDescription(description)
            .setTaskType(task.getTaskType())
            .setTaskParams(task)
            .setFlag(flag)
            .setTaskPriority(taskPriority)
            .setWorkerGroup("default")
            .setFailRetryTimes("0")
            .setFailRetryInterval("1")
            .setTimeoutFlag("CLOSE")
            .setTimeoutNotifyStrategy("WARN")
            .setIsCache("NO");
    Optional.ofNullable(cpuQuota).ifPresent(taskDefinition::setCpuQuota);
    Optional.ofNullable(memoryMax).ifPresent(taskDefinition::setMemoryMax);
    return taskDefinition;
  }
}
