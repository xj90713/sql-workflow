package com.xiaoxj.sqlworkflow.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

@Service
public class IngestInfoService {
    @Value("${postgres.url}")
    private String pgUrl;
    @Value("${postgres.username}")
    private String pgUser;
    @Value("${postgres.password}")
    private String pgPass;

    public List<String> findIngestTables(String sourceDb, String sourceTablename) {
        if (pgUrl == null || pgUrl.isEmpty()) return List.of();
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException ignored) {}
        List<String> res = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(pgUrl, pgUser, pgPass);
             PreparedStatement ps = conn.prepareStatement("SELECT table_name FROM VW_IMP_ETL vie WHERE sou_owner = ? AND sou_tablename = ? AND flag = 'Y'");) {
            ps.setString(1, sourceDb);
            ps.setString(2, sourceTablename);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) res.add(rs.getString(1));
            }
        } catch (Exception e) {
            return List.of();
        }
        return res;
    }
}
