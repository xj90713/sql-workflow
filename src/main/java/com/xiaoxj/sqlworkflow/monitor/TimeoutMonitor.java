//package com.xiaoxj.sqlworkflow.monitor;
//
//import com.xiaoxj.sqlworkflow.domain.TaskStatus;
//import com.xiaoxj.sqlworkflow.repo.TaskStatusRepository;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.scheduling.annotation.Scheduled;
//import org.springframework.stereotype.Component;
//
//import java.time.Duration;
//import java.time.Instant;
//
//@Component
//public class TimeoutMonitor {
//    private final TaskStatusRepository statusRepository;
//    private final NotificationService notificationService;
//    @Value("${monitor.timeoutSeconds:7200}")
//    private long timeoutSeconds;
//
//    public TimeoutMonitor(TaskStatusRepository statusRepository, NotificationService notificationService) {
//        this.statusRepository = statusRepository;
//        this.notificationService = notificationService;
//    }
//
//    @Scheduled(fixedDelay = 60000)
//    public void checkTimeouts() {
//        for (TaskStatus t : statusRepository.findAll()) {
//            if (t.getCurrentStatus() == TaskStatus.Status.RUNNING && t.getStartTime() != null) {
//                long elapsed = Duration.between(t.getStartTime(), Instant.now()).getSeconds();
//                if (elapsed > timeoutSeconds) {
//                    t.setCurrentStatus(TaskStatus.Status.FAILED);
//                    statusRepository.save(t);
//                    String msg = "任务超时: " + t.getTaskName() + ", 已运行" + elapsed + "秒";
//                    notificationService.sendDingTalk(msg);
//                }
//            }
//        }
//    }
//}
