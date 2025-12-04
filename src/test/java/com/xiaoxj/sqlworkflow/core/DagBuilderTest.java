package com.xiaoxj.sqlworkflow.core;

import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

public class DagBuilderTest {
    @Test
    void topoSortSimple() {
        Map<String, List<String>> edges = Map.of(
                "A", List.of("B"),
                "B", List.of("C"),
                "C", List.of()
        );
        List<String> order = DagBuilder.topoSort(edges);
        assertEquals(List.of("A","B","C"), order);
    }

    @Test
    void detectCycle() {
        Map<String, List<String>> edges = Map.of(
                "A", List.of("B"),
                "B", List.of("A")
        );
        assertThrows(IllegalStateException.class, () -> DagBuilder.topoSort(edges));
    }
}
