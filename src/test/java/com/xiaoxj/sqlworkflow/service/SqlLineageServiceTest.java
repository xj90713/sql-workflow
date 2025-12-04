//package com.xiaoxj.sqlworkflow.service;
//
//import com.xiaoxj.sqlworkflow.domain.TaskDependency;
//import com.xiaoxj.sqlworkflow.repo.TaskDeployRepository;
//import com.xiaoxj.sqlworkflow.repo.TaskDependencyRepository;
//import org.junit.jupiter.api.Test;
//import org.mockito.Mockito;
//
//import static org.junit.jupiter.api.Assertions.*;
//
//public class SqlLineageServiceTest {
//    @Test
//    void parseHeaderStyle() {
//        var deployRepo = Mockito.mock(TaskDeployRepository.class);
//        var depRepo = Mockito.mock(TaskDependencyRepository.class);
//        var svc = new SqlLineageService(deployRepo, depRepo);
//        String sql = "-- #########################################################\n"+
//                "-- 表名： cdm.dwd_target\n"+
//                "-- Output：输出表： cdm.dwd_target\n"+
//                "-- Input:  输入表：ods.src_a,pdw.dim_b\n"+
//                "insert overwrite table cdm.dwd_target select * from ods.src_a";
//        TaskDependency dep = svc.addTask("cdm.dwd_target","sql/tasks/cdm.dwd_target.sql","cdm.dwd_target.sql",sql,"tester");
//        assertEquals("cdm.dwd_target", dep.getTargetTable());
//        assertTrue(dep.getSourceTables().contains("ods.src_a"));
//    }
//}
