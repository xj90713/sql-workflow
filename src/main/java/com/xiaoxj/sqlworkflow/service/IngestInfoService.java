package com.xiaoxj.sqlworkflow.service;

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
public class IngestInfoService {
    @Value("${postgres.url}")
    private String pgUrl;
    @Value("${postgres.username}")
    private String pgUser;
    @Value("${postgres.password}")
    private String pgPass;

    public List<String> findIngestTables(String sourceDbs, String sourceTables) {
        sourceDbs = Arrays.stream(sourceDbs.split(","))
        .map(String::trim)
        .map(table -> "'" + table + "'")
        .collect(Collectors.joining(","));
        sourceTables = Arrays.stream(sourceTables.split(","))
        .map(String::trim)
        .map(table -> "'" + table + "'")
        .collect(Collectors.joining(","));
        if (pgUrl == null || pgUrl.isEmpty()) return List.of();
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException ignored) {}
        List<String> res = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(pgUrl, pgUser, pgPass);
             PreparedStatement ps = conn.prepareStatement("SELECT concat(target_db,'.',target_table)  FROM vw_etl_table_with_source  vie " +
                     "where vie.status='Y' and vie.target_db in (?) and vie.target_table in (?)");) {
            ps.setString(1, sourceDbs);
            ps.setString(2, sourceTables);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) res.add(rs.getString(1));
            }
        } catch (Exception e) {
            return List.of();
        }
        return res;
    }
}
