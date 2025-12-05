package com.xiaoxj.sqlworkflow.taskinstance;

import com.xiaoxj.sqlworkflow.BaseTest;
import org.junit.Test;

import java.util.List;

public class TaskInstanceTest extends BaseTest {

  @Test
  public void testPage() {
    Long workflowInstanceId = 1L;
    List<TaskInstanceQueryResp> taskInstanceQueryResps =
        getClient().opsForTaskInstance().page(projectCode, null, null, workflowInstanceId);

    taskInstanceQueryResps.forEach(System.out::println);
  }

  @Test
  public void testQueryLog() {
    Long taskInstanceId = 1L;
    String log = getClient().opsForTaskInstance().queryLog(projectCode, null, null, taskInstanceId);

    System.out.println(log);
  }
}
