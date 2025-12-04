//package com.xiaoxj.sqlworkflow.service;
//
//import com.xiaoxj.sqlworkflow.domain.TaskDeploy;
//import com.xiaoxj.sqlworkflow.domain.TaskDependency;
//import com.xiaoxj.sqlworkflow.repo.TaskDeployRepository;
//import com.xiaoxj.sqlworkflow.repo.TaskDependencyRepository;
//import org.junit.jupiter.api.Test;
//import org.mockito.Mockito;
//
//import java.util.List;
//
//import static org.junit.jupiter.api.Assertions.*;
//
//public class SqlLineageServiceUpdateTest {
//    @Test
//    void updateOnChangedSql() {
//        var deployRepo = Mockito.mock(TaskDeployRepository.class);
//        var depRepo = Mockito.mock(TaskDependencyRepository.class);
//        var svc = new SqlLineageService(deployRepo, depRepo);
//
//        TaskDeploy existing = new TaskDeploy();
//        existing.setTaskName("cdm.dwd_target");
//        existing.setFileMd5("oldmd5");
//        Mockito.when(deployRepo.findTopByTaskNameOrderByUpdateTimeDesc("cdm.dwd_target")).thenReturn(existing);
//        Mockito.when(depRepo.findByTaskName("cdm.dwd_target")).thenReturn(List.of());
//
//        String sql = "insert overwrite table cdm.dwd_target select * from ods.src_b";
//        TaskDependency dep = svc.updateTask("cdm.dwd_target","sql/tasks/cdm.dwd_target.sql","cdm.dwd_target.sql",sql,"tester");
//        assertEquals("cdm.dwd_target", dep.getTargetTable());
//        assertTrue(dep.getSourceTables().contains("ods.src_b"));
//        assertEquals("UPDATED", dep.getStatus());
//    }
//}
