package com.xiaoxj.sqlworkflow.workflow;


import com.xiaoxj.sqlworkflow.BaseTest;
import com.xiaoxj.sqlworkflow.dolphinscheduler.instance.WorkflowInstanceQueryResp;
import com.xiaoxj.sqlworkflow.dolphinscheduler.workflow.TaskDefinition;
import com.xiaoxj.sqlworkflow.dolphinscheduler.workflow.WrokflowDefineParam;
import com.xiaoxj.sqlworkflow.dolphinscheduler.workflow.WrokflowDefineResp;
import com.xiaoxj.sqlworkflow.enums.HttpCheckCondition;
import com.xiaoxj.sqlworkflow.remote.HttpMethod;
import com.xiaoxj.sqlworkflow.dolphinscheduler.task.HivecliTask;
import com.xiaoxj.sqlworkflow.dolphinscheduler.task.HttpTask;
import com.xiaoxj.sqlworkflow.dolphinscheduler.task.ShellTask;
import com.xiaoxj.sqlworkflow.util.TaskDefinitionUtils;
import com.xiaoxj.sqlworkflow.util.TaskLocationUtils;
import com.xiaoxj.sqlworkflow.util.TaskRelationUtils;
import com.xiaoxj.sqlworkflow.util.TaskUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/** the test for workflow */
public class WorkflowTest extends BaseTest {

  public static final String WORKFLOW_NAME = "test-dag1";

  /**
   * create simple workflow like: shellTask -> httpTask
   *
   * <p>1.generate task code
   *
   * <p>2.create tasks
   *
   * <p>3.create task definitions
   *
   * <p>4.create task relations
   *
   * <p>5.create workflow create parm
   *
   * <p>
   */
  @Test
  public void testCreateWorkflowDefinition() {

    List<Long> taskCodes = getClient().opsForWorkflow().generateTaskCode(projectCode, 3);
    System.out.println("test:" + taskCodes);
    Long[] array = taskCodes.toArray(new Long[0]);
    System.out.println("test:" + taskCodes);
    // build hivecli task
    HivecliTask hivecliTask = new HivecliTask();
    hivecliTask.setHiveSqlScript("show databases;");
    TaskDefinition hiveTaskDefinition =
            TaskDefinitionUtils.createDefaultTaskDefinition(taskCodes.get(0), hivecliTask);
    // build shell task
    ShellTask shellTask = new ShellTask();
    shellTask.setRawScript("echo 'hello dolphin scheduler java sdk'");
    TaskDefinition shellTaskDefinition =
        TaskDefinitionUtils.createDefaultTaskDefinition(taskCodes.get(1), shellTask);

    // build http task
    HttpTask httpTask = new HttpTask();
    httpTask
        .setUrl("http://www.baidu.com")
        .setHttpMethod(HttpMethod.GET.toString())
        .setHttpCheckCondition(HttpCheckCondition.STATUS_CODE_DEFAULT.toString())
        .setCondition("")
        .setConditionResult(TaskUtils.createEmptyConditionResult());
    TaskDefinition httpTaskDefinition =
        TaskDefinitionUtils.createDefaultTaskDefinition(taskCodes.get(2), httpTask);

    WrokflowDefineParam pcr = new WrokflowDefineParam();
    pcr.setName(WORKFLOW_NAME)
        .setLocations(TaskLocationUtils.horizontalLocation(taskCodes.toArray(new Long[0])))
        .setDescription("test-dag-description")
        .setTenantCode(tenantCode)
        .setTimeout("0")
        .setExecutionType(WrokflowDefineParam.EXECUTION_TYPE_PARALLEL)
        .setTaskDefinitionJson(Arrays.asList(hiveTaskDefinition,shellTaskDefinition, httpTaskDefinition))
        .setTaskRelationJson(TaskRelationUtils.oneLineRelation(taskCodes.toArray(new Long[0])))
        .setGlobalParams(null);

    System.out.println(getClient().opsForWorkflow().create(projectCode, pcr));
  }

  @Test
  public void testPage() {
    List<WrokflowDefineResp> page =
        getClient().opsForWorkflow().page(projectCode, null, null, WORKFLOW_NAME);
    int expectedWorkflowNumber = 1;
    Assert.assertEquals(expectedWorkflowNumber, page.size());
  }

  @Test
  public void testOnlineWorkflow() {
    List<WrokflowDefineResp> page =
        getClient().opsForWorkflow().page(projectCode, null, null, WORKFLOW_NAME);
    Assert.assertTrue(getClient().opsForWorkflow().online(projectCode, page.get(0).getCode()));
  }

  @Test
  public void testOfflineWorkflow() {
    List<WrokflowDefineResp> page =
        getClient().opsForWorkflow().page(projectCode, null, null, WORKFLOW_NAME);
    Assert.assertTrue(getClient().opsForWorkflow().offline(projectCode, page.get(0).getCode()));
  }

  /** the workflow must in offline state */
  @Test
  public void testDeleteWorkflow() {
    List<WrokflowDefineResp> page =
        getClient().opsForWorkflow().page(projectCode, null, null, WORKFLOW_NAME);
    Assert.assertTrue(getClient().opsForWorkflow().delete(projectCode, page.get(0).getCode()));
  }

  @Test
  public void testGetWorkflowInstance() {
    WorkflowInstanceQueryResp workflowInstanceStatus = getClient().opsForWorkflowInst().getWorkflowInstanceStatus(projectCode, 486L);
    System.out.println(workflowInstanceStatus);
  }
}
