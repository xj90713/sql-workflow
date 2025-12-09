package com.xiaoxj.sqlworkflow.dolphinscheduler.task;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class ConditionTask extends AbstractTask {

  @Override
  public String getTaskType() {
    return "CONDITIONS";
  }
}
