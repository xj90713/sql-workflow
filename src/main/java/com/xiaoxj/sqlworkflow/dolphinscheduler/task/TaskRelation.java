package com.xiaoxj.sqlworkflow.dolphinscheduler.task;

import com.xiaoxj.sqlworkflow.util.JacksonUtils;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class TaskRelation {

  private String name = "";

  private Long preTaskCode = 0L;

  private Integer preTaskVersion = 0;

  private Long postTaskCode;

  private Integer postTaskVersion = 0;

  private Integer conditionType = 0;

  private Integer conditionParams;

  /**
   *
   * @return object json string
   */
  @Override
  public String toString() {
    return JacksonUtils.toJSONString(this);
  }
}
