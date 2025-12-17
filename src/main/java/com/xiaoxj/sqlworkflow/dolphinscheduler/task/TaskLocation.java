package com.xiaoxj.sqlworkflow.dolphinscheduler.task;

import com.xiaoxj.sqlworkflow.util.JacksonUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
public class TaskLocation {

  private Long taskCode;

  private int x;

  private int y;

  /**
   *
   * @return object json string
   */
  @Override
  public String toString() {
    return JacksonUtils.toJSONString(this);
  }
}
