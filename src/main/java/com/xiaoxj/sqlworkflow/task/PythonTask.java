package com.xiaoxj.sqlworkflow.task;

import com.xiaoxj.sqlworkflow.process.Parameter;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import java.util.Collections;
import java.util.List;

/** reference: org.apache.dolphinscheduler.plugin.task.python.PythonParameters */
@EqualsAndHashCode(callSuper = true)
@Data
@Accessors(chain = true)
public class PythonTask extends AbstractTask {

  /** resource list */
  private List<TaskResource> resourceList = Collections.emptyList();

  private List<Parameter> localParams = Collections.emptyList();

  /** python script */
  private String rawScript;

  @Override
  public String getTaskType() {
    return "PYTHON";
  }
}
