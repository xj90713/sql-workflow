package com.xiaoxj.sqlworkflow.task;

import com.xiaoxj.sqlworkflow.workflow.Parameter;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import java.util.Collections;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
@Accessors(chain = true)
public class HivecliTask extends AbstractTask{

    private String hiveSqlScript;

    private String hiveCliTaskExecutionType = "SCRIPT";

    private String hiveCliOptions = null;

    /** resource list */
    private List<TaskResource> resourceList = Collections.emptyList();

    private List<Parameter> localParams = Collections.emptyList();

    @Override
    public String getTaskType() {
        return "HIVECLI";
    }
}
