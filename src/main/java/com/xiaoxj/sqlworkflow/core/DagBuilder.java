package com.xiaoxj.sqlworkflow.core;

import java.util.*;

public class DagBuilder {
    public static List<String> topoSort(Map<String, List<String>> edges) {
        Map<String, Integer> indegree = new HashMap<>();
        for (String node : edges.keySet()) {
            indegree.putIfAbsent(node, 0);
            for (String v : edges.getOrDefault(node, List.of())) {
                indegree.put(v, indegree.getOrDefault(v, 0) + 1);
            }
        }
        Deque<String> q = new ArrayDeque<>();
        for (Map.Entry<String, Integer> e : indegree.entrySet()) if (e.getValue() == 0) q.add(e.getKey());
        List<String> order = new ArrayList<>();
        while (!q.isEmpty()) {
            String u = q.remove();
            order.add(u);
            for (String v : edges.getOrDefault(u, List.of())) {
                int d = indegree.merge(v, -1, Integer::sum);
                if (d == 0) q.add(v);
            }
        }
        if (order.size() != indegree.size()) throw new IllegalStateException("Cycle detected");
        return order;
    }
}
