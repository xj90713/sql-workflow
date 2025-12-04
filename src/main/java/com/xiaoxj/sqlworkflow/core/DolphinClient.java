package com.xiaoxj.sqlworkflow.core;

import com.xiaoxj.sqlworkflow.instance.ProcessInstanceOperator;
import com.xiaoxj.sqlworkflow.process.ProcessOperator;
import com.xiaoxj.sqlworkflow.remote.DolphinsRestTemplate;
import com.xiaoxj.sqlworkflow.taskinstance.TaskInstanceOperator;

public class DolphinClient {

    private final DolphinsRestTemplate dolphinsRestTemplate;
    private final String dolphinAddress;
    private final String token;

    private ProcessOperator processOperator;
    private ProcessInstanceOperator processInstanceOperator;
    private TaskInstanceOperator taskInstanceOperator;

    public DolphinClient(
            String token, String dolphinAddress, DolphinsRestTemplate dolphinsRestTemplate) {
        this.token = token;
        this.dolphinAddress = dolphinAddress;
        this.dolphinsRestTemplate = dolphinsRestTemplate;
        this.initOperators();
    }

    public void initOperators() {
        this.processOperator =
                new ProcessOperator(this.dolphinAddress, this.token, this.dolphinsRestTemplate);
        this.processInstanceOperator =
                new ProcessInstanceOperator(this.dolphinAddress, this.token, this.dolphinsRestTemplate);
        this.taskInstanceOperator =
                new TaskInstanceOperator(this.dolphinAddress, this.token, this.dolphinsRestTemplate);
    }


    public ProcessOperator opsForProcess() {
        return this.processOperator;
    }

    public ProcessInstanceOperator opsForProcessInst() {
        return this.processInstanceOperator;
    }

    public TaskInstanceOperator opsForTaskInstance() {
        return this.taskInstanceOperator;
    }

}
