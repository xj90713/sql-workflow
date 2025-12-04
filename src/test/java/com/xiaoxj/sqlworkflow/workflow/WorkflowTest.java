package com.xiaoxj.sqlworkflow.workflow;


import com.xiaoxj.sqlworkflow.BaseTest;
import com.xiaoxj.sqlworkflow.enums.HttpCheckCondition;
import com.xiaoxj.sqlworkflow.process.ProcessDefineParam;
import com.xiaoxj.sqlworkflow.process.ProcessDefineResp;
import com.xiaoxj.sqlworkflow.process.TaskDefinition;
import com.xiaoxj.sqlworkflow.remote.HttpMethod;
import com.xiaoxj.sqlworkflow.task.HttpTask;
import com.xiaoxj.sqlworkflow.task.ShellTask;
import com.xiaoxj.sqlworkflow.util.TaskDefinitionUtils;
import com.xiaoxj.sqlworkflow.util.TaskLocationUtils;
import com.xiaoxj.sqlworkflow.util.TaskRelationUtils;
import com.xiaoxj.sqlworkflow.util.TaskUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/** the test for workflow/process */
public class WorkflowTest extends BaseTest {

  public static final String WORKFLOW_NAME = "test-dag2";

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
   * <p>5.create process create parm
   *
   * <p>
   */
  @Test
  public void testCreateProcessDefinition() {

    List<Long> taskCodes = getClient().opsForProcess().generateTaskCode(projectCode, 2);

    // build shell task
    ShellTask shellTask = new ShellTask();
    shellTask.setRawScript("echo 'hello dolphin scheduler java sdk'");
    TaskDefinition shellTaskDefinition =
        TaskDefinitionUtils.createDefaultTaskDefinition(taskCodes.get(0), shellTask);

    // build http task
    HttpTask httpTask = new HttpTask();
    httpTask
        .setUrl("http://www.baidu.com")
        .setHttpMethod(HttpMethod.GET.toString())
        .setHttpCheckCondition(HttpCheckCondition.STATUS_CODE_DEFAULT.toString())
        .setCondition("")
        .setConditionResult(TaskUtils.createEmptyConditionResult());
    TaskDefinition httpTaskDefinition =
        TaskDefinitionUtils.createDefaultTaskDefinition(taskCodes.get(1), httpTask);

    ProcessDefineParam pcr = new ProcessDefineParam();
    pcr.setName(WORKFLOW_NAME)
        .setLocations(TaskLocationUtils.horizontalLocation(taskCodes.toArray(new Long[0])))
        .setDescription("test-dag-description")
        .setTenantCode(tenantCode)
        .setTimeout("0")
        .setExecutionType(ProcessDefineParam.EXECUTION_TYPE_PARALLEL)
        .setTaskDefinitionJson(Arrays.asList(shellTaskDefinition, httpTaskDefinition))
        .setTaskRelationJson(TaskRelationUtils.oneLineRelation(taskCodes.toArray(new Long[0])))
        .setGlobalParams(null);

    System.out.println(getClient().opsForProcess().create(projectCode, pcr));
  }

  @Test
  public void testPage() {
    List<ProcessDefineResp> page =
        getClient().opsForProcess().page(projectCode, null, null, WORKFLOW_NAME);
    int expectedWorkflowNumber = 1;
    Assert.assertEquals(expectedWorkflowNumber, page.size());
  }

  @Test
  public void testOnlineWorkflow() {
    List<ProcessDefineResp> page =
        getClient().opsForProcess().page(projectCode, null, null, WORKFLOW_NAME);
    Assert.assertTrue(getClient().opsForProcess().online(projectCode, page.get(0).getCode()));
  }

  @Test
  public void testOfflineWorkflow() {
    List<ProcessDefineResp> page =
        getClient().opsForProcess().page(projectCode, null, null, WORKFLOW_NAME);
    Assert.assertTrue(getClient().opsForProcess().offline(projectCode, page.get(0).getCode()));
  }

  /** the workflow must in offline state */
  @Test
  public void testDeleteWorkflow() {
    List<ProcessDefineResp> page =
        getClient().opsForProcess().page(projectCode, null, null, WORKFLOW_NAME);
    Assert.assertTrue(getClient().opsForProcess().delete(projectCode, page.get(0).getCode()));
  }
}
