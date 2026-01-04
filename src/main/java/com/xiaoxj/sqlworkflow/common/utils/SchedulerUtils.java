package com.xiaoxj.sqlworkflow.common.utils;

import com.xiaoxj.sqlworkflow.core.DolphinClient;
import com.xiaoxj.sqlworkflow.dolphinscheduler.schedule.ScheduleDefineParam;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Component
public class SchedulerUtils {
    private  final DolphinClient dolphinClient;

    @Value("${dolphin.tenant.code}")
    private String tenantCode;

    public SchedulerUtils(DolphinClient dolphinClient) {
        this.dolphinClient = dolphinClient;
    }

    public ScheduleDefineParam createScheduleDefineParam(Long projectCode, Long workflowCode, String schedule) {
        List<Long> taskCodes = dolphinClient.opsForWorkflow().generateTaskCode(projectCode, 2);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        ScheduleDefineParam scheduleDefineParam = new ScheduleDefineParam();
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime end = now.plusYears(100);

        scheduleDefineParam
                .setWorkflowDefinitionCode(workflowCode)
                .setTenantCode(tenantCode)
                .setSchedule(
                        new ScheduleDefineParam.Schedule()
                                .setStartTime(now.format(formatter))
                                .setEndTime(end.format(formatter))
                                .setCrontab(schedule));
        return scheduleDefineParam;
    }
}
