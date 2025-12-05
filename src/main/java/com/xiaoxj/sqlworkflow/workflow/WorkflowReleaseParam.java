package com.xiaoxj.sqlworkflow.workflow;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class WorkflowReleaseParam {

  public static final String ONLINE_STATE = "ONLINE";
  public static final String OFFLINE_STATE = "OFFLINE";

  /** workflow name, this field is not necessary, the dolphin scheduler rest api is shit!!! */
  private String name;

  /** workflow state： ONLINE or OFFLINE */
  private String releaseState;

  /**
   * create instance with online state
   *
   * @return {@link WorkflowReleaseParam} with online state
   */
  public static WorkflowReleaseParam newOnlineInstance() {
    return new WorkflowReleaseParam().setReleaseState(ONLINE_STATE);
  }

  /**
   * create instance with offline state
   *
   * @return {@link WorkflowReleaseParam} with offline state
   */
  public static WorkflowReleaseParam newOfflineInstance() {
    return new WorkflowReleaseParam().setReleaseState(OFFLINE_STATE);
  }
}
