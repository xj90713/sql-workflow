package com.xiaoxj.sqlworkflow.core;

import com.xiaoxj.sqlworkflow.instance.WorkflowInstanceOperator;
import com.xiaoxj.sqlworkflow.workflow.WorkflowOperator;
import com.xiaoxj.sqlworkflow.project.ProjectOperator;
import com.xiaoxj.sqlworkflow.remote.DolphinsRestTemplate;
import com.xiaoxj.sqlworkflow.taskinstance.TaskInstanceOperator;

public class DolphinClient {

    private final DolphinsRestTemplate dolphinsRestTemplate;
    private final String dolphinAddress;
    private final String token;

    private WorkflowOperator workflowOperator;
    private WorkflowInstanceOperator workflowInstanceOperator;
    private TaskInstanceOperator taskInstanceOperator;
    private ProjectOperator projectOperator;


    public DolphinClient(
            String token, String dolphinAddress, DolphinsRestTemplate dolphinsRestTemplate) {
        this.token = token;
        this.dolphinAddress = dolphinAddress;
        this.dolphinsRestTemplate = dolphinsRestTemplate;
        this.initOperators();
    }

    public void initOperators() {
        this.workflowOperator =
                new WorkflowOperator(this.dolphinAddress, this.token, this.dolphinsRestTemplate);
        this.workflowInstanceOperator =
                new WorkflowInstanceOperator(this.dolphinAddress, this.token, this.dolphinsRestTemplate);
        this.projectOperator =
                new ProjectOperator(this.dolphinAddress, this.token, this.dolphinsRestTemplate);
        this.taskInstanceOperator =
                new TaskInstanceOperator(this.dolphinAddress, this.token, this.dolphinsRestTemplate);
    }


    public WorkflowOperator opsForWorkflow() {
        return this.workflowOperator;
    }

    public WorkflowInstanceOperator opsForWorkflowInst() {
        return this.workflowInstanceOperator;
    }

    public TaskInstanceOperator opsForTaskInstance() {
        return this.taskInstanceOperator;
    }

    public ProjectOperator opsForProject() {
        return this.projectOperator;
    }
}
