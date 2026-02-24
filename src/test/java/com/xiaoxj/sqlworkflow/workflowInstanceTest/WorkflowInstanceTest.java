package com.xiaoxj.sqlworkflow.workflowInstanceTest;

import com.fasterxml.jackson.databind.JsonNode;
import com.xiaoxj.sqlworkflow.BaseTest;
import com.xiaoxj.sqlworkflow.dolphinscheduler.instance.WorkflowInstanceQueryResp;
import com.xiaoxj.sqlworkflow.remote.HttpRestResult;
import com.xiaoxj.sqlworkflow.service.DolphinSchedulerService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import java.util.List;

@SpringBootTest
@RunWith(SpringRunner.class)
public class WorkflowInstanceTest extends BaseTest {
    @Autowired
    private DolphinSchedulerService dolphinSchedulerService;
    @Test
    public void testStartWorkflow() {
        Long workflowCode = 160197424339552L;
        HttpRestResult<JsonNode> jsonNodeHttpRestResult = dolphinSchedulerService.startWorkflow(projectCode, workflowCode);
        System.out.println("workflowCode:" + jsonNodeHttpRestResult.getData().get("workflowCode"));
        System.out.println("workflowInstanceId:" + jsonNodeHttpRestResult.getData().get("workflowInstanceId").asLong());

        Long workflowInstanceId = Long.parseLong(jsonNodeHttpRestResult.getData().get("workflowInstanceId").toString());
        Long workflowCodetest = Long.parseLong(jsonNodeHttpRestResult.getData().get("workflowCode").toString());
        System.out.println("workflowCodetest:" + workflowCodetest);
        System.out.println("workflowInstanceId:" + workflowInstanceId);
        System.out.println(jsonNodeHttpRestResult);
    }

    @Test
    public void testStartWorkflows() {
        String workflowCodes = "159572138550880,159643457921632,159571583941216";
        List<HttpRestResult<JsonNode>> httpRestResults = dolphinSchedulerService.startWorkflows(projectCode, workflowCodes);
        System.out.println("httpRestResults:" + httpRestResults);
    }

    @Test
    public void testGetWorkflowInstance() {
        String workflowInstanceStatus = dolphinSchedulerService.getWorkflowInstanceStatus(projectCode, 500L);
        System.out.println(workflowInstanceStatus);
    }

    @Test
    public void testGetPage() {
        int nums = 0;
        while (true) {
            String page = getClient().opsForWorkflowInst().getWorkflowInstanceIds(1, 100, projectCode, 12);
            System.out.println( "page:" + page);
            nums += 100;
        Boolean b = getClient().opsForWorkflowInst().batchDelete(projectCode, page);

        System.out.println( page);
            if (page.isEmpty()) {
                System.out.println( "nums:" + nums);
                break;
            }

        }
    }

    @Test
    public void testQueryLog() {
        Long taskInstanceId = 1L;
        String log = getClient().opsForTaskInstance().queryLog(projectCode, null, null, taskInstanceId);

        System.out.println(log);
    }
}
