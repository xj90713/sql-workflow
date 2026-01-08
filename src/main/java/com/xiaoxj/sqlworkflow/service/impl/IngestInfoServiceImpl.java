package com.xiaoxj.sqlworkflow.service.impl;

import com.xiaoxj.sqlworkflow.repository.NoSchedulerTableRepository;
import com.xiaoxj.sqlworkflow.service.IngestInfoService;
import jakarta.annotation.Resource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class IngestInfoServiceImpl implements IngestInfoService {
    @Value("${postgres.url}")
    private String pgUrl;
    @Value("${postgres.username}")
    private String pgUser;
    @Value("${postgres.password}")
    private String pgPass;

    @Resource
    NoSchedulerTableRepository noSchedulerTableRepository;

    @Override
    public List<String> findIngestTables(String sourceDbs, String sourceTables) {
        if (pgUrl == null || pgUrl.isBlank()) return List.of();

        List<String> res = new ArrayList<>();

        List<String> dbList = Arrays.stream((sourceDbs == null ? "" : sourceDbs).split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
        List<String> tableList = Arrays.stream((sourceTables == null ? "" : sourceTables).split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();

        if (dbList.isEmpty() || tableList.isEmpty()) {
            res.addAll(noSchedulerTableRepository.findTableNamesByDeleteStatusNative());
            return res;
        }

        String dbPlaceholders = dbList.stream().map(d -> "?").collect(Collectors.joining(","));
        String tablePlaceholders = tableList.stream().map(t -> "?").collect(Collectors.joining(","));

        String sql = "SELECT concat(target_db,'.',target_table) FROM vw_etl_table_with_source vie " +
                "WHERE vie.status='Y' AND vie.target_db IN (" + dbPlaceholders + ") AND vie.target_table IN (" + tablePlaceholders + ")";

        try (Connection conn = DriverManager.getConnection(pgUrl, pgUser, pgPass);
             PreparedStatement ps = conn.prepareStatement(sql)) {

            int idx = 1;
            for (String db : dbList) ps.setString(idx++, db);
            for (String tbl : tableList) ps.setString(idx++, tbl);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) res.add(rs.getString(1));
            }
        } catch (Exception e) {
            log.error("查询入库表信息时发生异常", e);
            return List.of();
        }
        noSchedulerTableRepository.findTableNamesByDeleteStatusNative().forEach(t -> {
            if (!res.contains(t)) res.add(t);
        });
        return res;
    }
}
