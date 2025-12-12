package com.xiaoxj.sqlworkflow.dolphinscheduler.task;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

@EqualsAndHashCode(callSuper = true)
@Data
@Accessors(chain = true)
public class ConditionTask extends AbstractTask {

  @Override
  public String getTaskType() {
    return "CONDITIONS";
  }
}
