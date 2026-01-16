package com.xiaoxj.sqlworkflow.service;

import com.xiaoxj.sqlworkflow.common.utils.TextUtils;
import com.xiaoxj.sqlworkflow.entity.AlertWorkflowDeploy;
import com.xiaoxj.sqlworkflow.entity.WorkflowDeploy;
import com.xiaoxj.sqlworkflow.repository.AlertWorkflowDeployRepository;
import com.xiaoxj.sqlworkflow.repository.WorkflowDeployRepository;
import io.github.reata.sqllineage4j.common.model.Table;
import io.github.reata.sqllineage4j.core.LineageRunner;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.time.LocalDateTime;

public interface SqlLineageService {
    WorkflowDeploy addWorkflowDeploy(String workflowName, String filePath, String fileName, String sqlContent, String commitUser, long workflowCode, long projectCode, String taskCodes);
    WorkflowDeploy updateWorkflowDeploy(String workflowName, String filePath, String fileName, String sqlContent, String commitUser, long workflowCode, long projectCode, String taskCodes);
    Map<String, String> getTargetAndSourceTablesOrDepedencies(String sqlContent, String fileName);
    AlertWorkflowDeploy addAlertWorkflowDeploy(String workflowName, String filePath, String fileName, String sqlContent, String commitUser, String taskCodes,long scheduleId, long workflowCode, long projectCode, String scheduleTime);
    AlertWorkflowDeploy updateAlertWorkflowDeploy(String workflowName, String filePath, String fileName, String sqlContent, String commitUser, String taskCodes,long scheduleId,long workflowCode, long projectCode, String scheduleTime);
}
