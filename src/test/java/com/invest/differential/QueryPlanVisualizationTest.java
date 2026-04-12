package com.invest.differential;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class QueryPlanVisualizationTest {

    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    @Test
    void basicFilterProjectDot() {
        Schema schema = new Schema(List.of(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("SELECT name, val FROM t WHERE val > 10");

            String dot = engine.getCircuit().toDot();
            String dotUpper = dot.toUpperCase();
            assertTrue(dot.startsWith("digraph Circuit {"));
            assertTrue(dotUpper.contains("INPUT(T)"));
            assertTrue(dotUpper.contains("OUTPUT"));
            assertTrue(dotUpper.contains("FILTER"));
            assertTrue(dot.contains("->"));
            assertTrue(dot.endsWith("}\n"));
        }
    }

    @Test
    void joinDotHasTwoInputs() {
        Schema left = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)
        ));
        Schema right = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("score", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("users", left)
                    .registerTable("scores", right)
                    .sql("SELECT u.name, s.score FROM users u INNER JOIN scores s ON u.id = s.id");

            String dot = engine.getCircuit().toDot();
            String dotUpper = dot.toUpperCase();
            assertTrue(dotUpper.contains("INPUT(USERS)"));
            assertTrue(dotUpper.contains("INPUT(SCORES)"));
            assertTrue(dotUpper.contains("INCREMENTALJOIN"));

            // Count edges going into the join node
            int joinIdx = findNodeIndexContaining(dot, "IncrementalJoin");
            assertTrue(joinIdx >= 0, "Should find IncrementalJoin node in: " + dot);
            int edgesToJoin = countEdgesTo(dot, joinIdx);
            assertEquals(2, edgesToJoin, "Join should have 2 input edges");
        }
    }

    @Test
    void aggregationDotChain() {
        Schema schema = new Schema(List.of(
                new Field("dept", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("salary", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("emp", schema)
                    .sql("SELECT dept, SUM(salary) FROM emp GROUP BY dept");

            String dot = engine.getCircuit().toDot();
            String dotUpper = dot.toUpperCase();
            assertTrue(dotUpper.contains("INPUT(EMP)"));
            assertTrue(dotUpper.contains("INCREMENTALAGGREGATE"));
            assertTrue(countEdges(dot) >= 3, "Aggregate pipeline should have multiple edges");
        }
    }

    @Test
    void unionAllDot() {
        Schema schema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("a", schema)
                    .registerTable("b", schema)
                    .sql("SELECT id FROM a UNION ALL SELECT id FROM b");

            String dot = engine.getCircuit().toDot();
            String dotUpper = dot.toUpperCase();
            assertTrue(dotUpper.contains("INPUT(A)"));
            assertTrue(dotUpper.contains("INPUT(B)"));
            assertTrue(dotUpper.contains("UNIONALL"));
        }
    }

    @Test
    void exceptDot() {
        Schema schema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("a", schema)
                    .registerTable("b", schema)
                    .sql("SELECT id FROM a EXCEPT SELECT id FROM b");

            String dot = engine.getCircuit().toDot();
            assertTrue(dot.toUpperCase().contains("EXCEPT"));
        }
    }

    @Test
    void complexQueryDotNodeCount() {
        Schema schema = new Schema(List.of(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("dept", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("salary", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("emp", schema)
                    .sql("SELECT dept, COUNT(*) FROM emp WHERE salary > 50000 GROUP BY dept");

            String dot = engine.getCircuit().toDot();
            // Should have Input -> ... -> Filter -> ... -> Aggregate -> ... -> Output
            int nodeCount = countNodes(dot);
            assertTrue(nodeCount >= 4, "Complex query should have at least 4 operators, got " + nodeCount);
        }
    }

    @Test
    void dotIsValidGraphvizSyntax() {
        Schema schema = new Schema(List.of(
                new Field("x", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("SELECT x FROM t");

            String dot = engine.getCircuit().toDot();
            // Basic syntax checks
            assertTrue(dot.startsWith("digraph Circuit {"));
            assertTrue(dot.endsWith("}\n"));
            assertTrue(dot.contains("rankdir=TB"));
            assertTrue(dot.contains("node ["));
            // All node references should be well-formed
            assertFalse(dot.contains("op-1"));
        }
    }

    @Test
    void multiQueryDot() {
        Schema schema = new Schema(List.of(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        try (IncrementalEngine engine = IncrementalEngine.create(allocator)) {
            engine.registerTable("t", schema)
                    .sql("SELECT name, val FROM t WHERE val > 10", "view1")
                    .sql("SELECT name, val FROM t WHERE val <= 10", "view2");

            String dot = engine.getCircuit().toDot();
            String dotUpper = dot.toUpperCase();
            assertTrue(dotUpper.contains("INPUT(T)"));
            // Both views are compiled into the circuit with separate operator chains
            int outputCount = 0;
            for (String line : dot.split("\n")) {
                if (line.contains("Output(")) outputCount++;
            }
            assertEquals(2, outputCount, "Should have 2 output operators");
            // Verify shared input feeds both chains
            assertTrue(countEdges(dot) >= 4, "Should have edges for both query chains");
        }
    }

    // --- Helpers ---

    private int findNodeIndex(String dot, String label) {
        String[] lines = dot.split("\n");
        for (String line : lines) {
            if (line.contains("label=\"" + label + "\"")) {
                int start = line.indexOf("op") + 2;
                int end = line.indexOf(" ", start);
                return Integer.parseInt(line.substring(start, end));
            }
        }
        return -1;
    }

    private int findNodeIndexContaining(String dot, String labelPart) {
        String[] lines = dot.split("\n");
        for (String line : lines) {
            if (line.contains("label=") && line.contains(labelPart)) {
                int start = line.indexOf("op") + 2;
                int end = line.indexOf(" ", start);
                return Integer.parseInt(line.substring(start, end));
            }
        }
        return -1;
    }

    private int countEdgesTo(String dot, int targetIdx) {
        int count = 0;
        String target = " -> op" + targetIdx + ";";
        for (String line : dot.split("\n")) {
            if (line.trim().endsWith(target.trim())) count++;
        }
        return count;
    }

    private int countEdges(String dot) {
        int count = 0;
        for (String line : dot.split("\n")) {
            if (line.trim().contains("->")) count++;
        }
        return count;
    }

    private int countNodes(String dot) {
        int count = 0;
        for (String line : dot.split("\n")) {
            if (line.trim().startsWith("op") && line.contains("label=")) count++;
        }
        return count;
    }
}
