package com.xiaoxj.sqlworkflow.service;

import com.xiaoxj.sqlworkflow.core.DolphinClient;
import com.xiaoxj.sqlworkflow.workflow.WrokflowDefineParam;
import com.xiaoxj.sqlworkflow.workflow.WrokflowDefineResp;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DolphinSchedulerService {
    private final DolphinClient dolphinClient;

    public DolphinSchedulerService(DolphinClient dolphinClient) {
        this.dolphinClient = dolphinClient;
    }

    public List<Long> generateTaskCodes(Long projectCode, int count) {
        return dolphinClient.opsForProcess().generateTaskCode(projectCode, count);
    }

    public List<WrokflowDefineResp> listWorkflows(Long projectCode, Integer pageNo, Integer pageSize, String searchVal) {
        return dolphinClient.opsForProcess().page(projectCode, pageNo, pageSize, searchVal);
    }

    public WrokflowDefineResp createWorkflow(Long projectCode, WrokflowDefineParam param) {
        return dolphinClient.opsForProcess().create(projectCode, param);
    }

    public WrokflowDefineResp updateWorkflow(Long projectCode, Long workflowCode, WrokflowDefineParam param) {
        return dolphinClient.opsForProcess().update(projectCode, param, workflowCode);
    }

    public boolean deleteWorkflow(Long projectCode, Long workflowCode) {
        return dolphinClient.opsForProcess().delete(projectCode, workflowCode);
    }
    public boolean onlineWorkflow(Long projectCode, Long workflowCode) {
        return dolphinClient.opsForProcess().online(projectCode, workflowCode);
    }

    public boolean offlineWorkflow(Long projectCode, Long workflowCode) {
        return dolphinClient.opsForProcess().offline(projectCode, workflowCode);
    }
}
