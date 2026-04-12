package com.invest.differential;

import com.invest.differential.expr.VectorizedEvaluator;
import com.invest.differential.expr.VectorizedExpressions;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class VectorizedExpressionTest {

    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    private VectorSchemaRoot createIntBatch(String name, int... values) {
        Schema schema = new Schema(List.of(
                new Field(name, FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        IntVector v = (IntVector) root.getVector(0);
        for (int i = 0; i < values.length; i++) v.set(i, values[i]);
        root.setRowCount(values.length);
        return root;
    }

    private VectorSchemaRoot createTwoIntBatch(int[] col0, int[] col1) {
        Schema schema = new Schema(List.of(
                new Field("a", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("b", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        IntVector v0 = (IntVector) root.getVector(0);
        IntVector v1 = (IntVector) root.getVector(1);
        for (int i = 0; i < col0.length; i++) {
            v0.set(i, col0[i]);
            v1.set(i, col1[i]);
        }
        root.setRowCount(col0.length);
        return root;
    }

    private VectorSchemaRoot createStringBatch(String... values) {
        Schema schema = new Schema(List.of(
                new Field("s", FieldType.nullable(new ArrowType.Utf8()), null)
        ));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        VarCharVector v = (VarCharVector) root.getVector(0);
        for (int i = 0; i < values.length; i++) {
            v.set(i, values[i].getBytes(StandardCharsets.UTF_8));
        }
        root.setRowCount(values.length);
        return root;
    }

    // ── Field Reference ──

    @Test
    void fieldRefCopiesColumn() {
        try (VectorSchemaRoot batch = createIntBatch("x", 10, 20, 30)) {
            VectorizedEvaluator eval = VectorizedExpressions.fieldRef(0);
            try (FieldVector result = eval.evaluate(batch, allocator)) {
                IntVector iv = (IntVector) result;
                assertEquals(3, iv.getValueCount());
                assertEquals(10, iv.get(0));
                assertEquals(20, iv.get(1));
                assertEquals(30, iv.get(2));
            }
        }
    }

    // ── Arithmetic ──

    @Test
    void addTwoColumns() {
        try (VectorSchemaRoot batch = createTwoIntBatch(
                new int[]{1, 2, 3, 4},
                new int[]{10, 20, 30, 40})) {
            VectorizedEvaluator eval = VectorizedExpressions.add(
                    VectorizedExpressions.fieldRef(0),
                    VectorizedExpressions.fieldRef(1)
            );
            try (FieldVector result = eval.evaluate(batch, allocator)) {
                IntVector iv = (IntVector) result;
                assertEquals(4, iv.getValueCount());
                assertEquals(11, iv.get(0));
                assertEquals(22, iv.get(1));
                assertEquals(33, iv.get(2));
                assertEquals(44, iv.get(3));
            }
        }
    }

    @Test
    void subtractColumnFromLiteral() {
        try (VectorSchemaRoot batch = createIntBatch("x", 5, 10, 15)) {
            VectorizedEvaluator eval = VectorizedExpressions.subtract(
                    VectorizedExpressions.intLiteral(100),
                    VectorizedExpressions.fieldRef(0)
            );
            try (FieldVector result = eval.evaluate(batch, allocator)) {
                IntVector iv = (IntVector) result;
                assertEquals(95, iv.get(0));
                assertEquals(90, iv.get(1));
                assertEquals(85, iv.get(2));
            }
        }
    }

    @Test
    void multiplyColumns() {
        try (VectorSchemaRoot batch = createTwoIntBatch(
                new int[]{2, 3, 4},
                new int[]{5, 6, 7})) {
            VectorizedEvaluator eval = VectorizedExpressions.multiply(
                    VectorizedExpressions.fieldRef(0),
                    VectorizedExpressions.fieldRef(1)
            );
            try (FieldVector result = eval.evaluate(batch, allocator)) {
                IntVector iv = (IntVector) result;
                assertEquals(10, iv.get(0));
                assertEquals(18, iv.get(1));
                assertEquals(28, iv.get(2));
            }
        }
    }

    @Test
    void divideColumns() {
        try (VectorSchemaRoot batch = createTwoIntBatch(
                new int[]{100, 200, 300},
                new int[]{10, 20, 30})) {
            VectorizedEvaluator eval = VectorizedExpressions.divide(
                    VectorizedExpressions.fieldRef(0),
                    VectorizedExpressions.fieldRef(1)
            );
            try (FieldVector result = eval.evaluate(batch, allocator)) {
                IntVector iv = (IntVector) result;
                assertEquals(10, iv.get(0));
                assertEquals(10, iv.get(1));
                assertEquals(10, iv.get(2));
            }
        }
    }

    @Test
    void negateColumn() {
        try (VectorSchemaRoot batch = createIntBatch("x", 5, -3, 0)) {
            VectorizedEvaluator eval = VectorizedExpressions.negate(
                    VectorizedExpressions.fieldRef(0)
            );
            try (FieldVector result = eval.evaluate(batch, allocator)) {
                IntVector iv = (IntVector) result;
                assertEquals(-5, iv.get(0));
                assertEquals(3, iv.get(1));
                assertEquals(0, iv.get(2));
            }
        }
    }

    // ── Comparison ──

    @Test
    void greaterThanFilter() {
        try (VectorSchemaRoot batch = createIntBatch("x", 1, 5, 10, 15, 20)) {
            VectorizedEvaluator pred = VectorizedExpressions.gt(
                    VectorizedExpressions.fieldRef(0),
                    VectorizedExpressions.intLiteral(10)
            );
            try (FieldVector result = pred.evaluate(batch, allocator)) {
                BitVector bits = (BitVector) result;
                assertEquals(5, bits.getValueCount());
                assertEquals(0, bits.get(0)); // 1 > 10 = false
                assertEquals(0, bits.get(1)); // 5 > 10 = false
                assertEquals(0, bits.get(2)); // 10 > 10 = false
                assertEquals(1, bits.get(3)); // 15 > 10 = true
                assertEquals(1, bits.get(4)); // 20 > 10 = true
            }
        }
    }

    @Test
    void equalComparison() {
        try (VectorSchemaRoot batch = createIntBatch("x", 1, 2, 3, 2, 1)) {
            VectorizedEvaluator pred = VectorizedExpressions.equal(
                    VectorizedExpressions.fieldRef(0),
                    VectorizedExpressions.intLiteral(2)
            );
            try (FieldVector result = pred.evaluate(batch, allocator)) {
                BitVector bits = (BitVector) result;
                assertEquals(0, bits.get(0));
                assertEquals(1, bits.get(1));
                assertEquals(0, bits.get(2));
                assertEquals(1, bits.get(3));
                assertEquals(0, bits.get(4));
            }
        }
    }

    // ── Boolean ──

    @Test
    void andCombination() {
        try (VectorSchemaRoot batch = createIntBatch("x", 5, 10, 15, 20, 25)) {
            // x > 8 AND x < 22
            VectorizedEvaluator pred = VectorizedExpressions.and(
                    VectorizedExpressions.gt(
                            VectorizedExpressions.fieldRef(0),
                            VectorizedExpressions.intLiteral(8)
                    ),
                    VectorizedExpressions.lt(
                            VectorizedExpressions.fieldRef(0),
                            VectorizedExpressions.intLiteral(22)
                    )
            );
            try (FieldVector result = pred.evaluate(batch, allocator)) {
                BitVector bits = (BitVector) result;
                assertEquals(0, bits.get(0)); // 5: false
                assertEquals(1, bits.get(1)); // 10: true
                assertEquals(1, bits.get(2)); // 15: true
                assertEquals(1, bits.get(3)); // 20: true
                assertEquals(0, bits.get(4)); // 25: false
            }
        }
    }

    @Test
    void orCombination() {
        try (VectorSchemaRoot batch = createIntBatch("x", 1, 5, 10, 15, 20)) {
            // x < 3 OR x > 18
            VectorizedEvaluator pred = VectorizedExpressions.or(
                    VectorizedExpressions.lt(
                            VectorizedExpressions.fieldRef(0),
                            VectorizedExpressions.intLiteral(3)
                    ),
                    VectorizedExpressions.gt(
                            VectorizedExpressions.fieldRef(0),
                            VectorizedExpressions.intLiteral(18)
                    )
            );
            try (FieldVector result = pred.evaluate(batch, allocator)) {
                BitVector bits = (BitVector) result;
                assertEquals(1, bits.get(0)); // 1 < 3
                assertEquals(0, bits.get(1)); // neither
                assertEquals(0, bits.get(2)); // neither
                assertEquals(0, bits.get(3)); // neither
                assertEquals(1, bits.get(4)); // 20 > 18
            }
        }
    }

    @Test
    void notInversion() {
        try (VectorSchemaRoot batch = createIntBatch("x", 1, 10, 20)) {
            VectorizedEvaluator pred = VectorizedExpressions.not(
                    VectorizedExpressions.gt(
                            VectorizedExpressions.fieldRef(0),
                            VectorizedExpressions.intLiteral(5)
                    )
            );
            try (FieldVector result = pred.evaluate(batch, allocator)) {
                BitVector bits = (BitVector) result;
                assertEquals(1, bits.get(0)); // NOT(1>5) = true
                assertEquals(0, bits.get(1)); // NOT(10>5) = false
                assertEquals(0, bits.get(2)); // NOT(20>5) = false
            }
        }
    }

    // ── String operations ──

    @Test
    void upperVectorized() {
        try (VectorSchemaRoot batch = createStringBatch("hello", "World", "TEST")) {
            VectorizedEvaluator eval = VectorizedExpressions.upper(
                    VectorizedExpressions.fieldRef(0)
            );
            try (FieldVector result = eval.evaluate(batch, allocator)) {
                VarCharVector sv = (VarCharVector) result;
                assertEquals("HELLO", new String(sv.get(0), StandardCharsets.UTF_8));
                assertEquals("WORLD", new String(sv.get(1), StandardCharsets.UTF_8));
                assertEquals("TEST", new String(sv.get(2), StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    void lowerVectorized() {
        try (VectorSchemaRoot batch = createStringBatch("HELLO", "World", "test")) {
            VectorizedEvaluator eval = VectorizedExpressions.lower(
                    VectorizedExpressions.fieldRef(0)
            );
            try (FieldVector result = eval.evaluate(batch, allocator)) {
                VarCharVector sv = (VarCharVector) result;
                assertEquals("hello", new String(sv.get(0), StandardCharsets.UTF_8));
                assertEquals("world", new String(sv.get(1), StandardCharsets.UTF_8));
                assertEquals("test", new String(sv.get(2), StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    void charLengthVectorized() {
        try (VectorSchemaRoot batch = createStringBatch("hi", "hello", "")) {
            VectorizedEvaluator eval = VectorizedExpressions.charLength(
                    VectorizedExpressions.fieldRef(0)
            );
            try (FieldVector result = eval.evaluate(batch, allocator)) {
                IntVector iv = (IntVector) result;
                assertEquals(2, iv.get(0));
                assertEquals(5, iv.get(1));
                assertEquals(0, iv.get(2));
            }
        }
    }

    // ── Batch filter ──

    @Test
    void filterBatchSelectsMatchingRows() {
        try (VectorSchemaRoot batch = createTwoIntBatch(
                new int[]{1, 2, 3, 4, 5},
                new int[]{10, 20, 30, 40, 50})) {
            // Filter: a > 2
            VectorizedEvaluator pred = VectorizedExpressions.gt(
                    VectorizedExpressions.fieldRef(0),
                    VectorizedExpressions.intLiteral(2)
            );
            try (VectorSchemaRoot filtered = VectorizedExpressions.filterBatch(
                    batch, pred, allocator)) {
                assertEquals(3, filtered.getRowCount());
                IntVector a = (IntVector) filtered.getVector(0);
                IntVector b = (IntVector) filtered.getVector(1);
                assertEquals(3, a.get(0)); assertEquals(30, b.get(0));
                assertEquals(4, a.get(1)); assertEquals(40, b.get(1));
                assertEquals(5, a.get(2)); assertEquals(50, b.get(2));
            }
        }
    }

    @Test
    void filterBatchWithCompoundPredicate() {
        try (VectorSchemaRoot batch = createTwoIntBatch(
                new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
                new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0})) {
            // Filter: a >= 3 AND a <= 7
            VectorizedEvaluator pred = VectorizedExpressions.and(
                    VectorizedExpressions.gte(
                            VectorizedExpressions.fieldRef(0),
                            VectorizedExpressions.intLiteral(3)
                    ),
                    VectorizedExpressions.lte(
                            VectorizedExpressions.fieldRef(0),
                            VectorizedExpressions.intLiteral(7)
                    )
            );
            try (VectorSchemaRoot filtered = VectorizedExpressions.filterBatch(
                    batch, pred, allocator)) {
                assertEquals(5, filtered.getRowCount());
                IntVector a = (IntVector) filtered.getVector(0);
                assertEquals(3, a.get(0));
                assertEquals(7, a.get(4));
            }
        }
    }

    // ── Batch projection ──

    @Test
    void projectBatchComputesNewColumns() {
        try (VectorSchemaRoot batch = createTwoIntBatch(
                new int[]{10, 20, 30},
                new int[]{1, 2, 3})) {
            // Project: a + b, a * b
            List<VectorizedEvaluator> columns = List.of(
                    VectorizedExpressions.add(
                            VectorizedExpressions.fieldRef(0),
                            VectorizedExpressions.fieldRef(1)
                    ),
                    VectorizedExpressions.multiply(
                            VectorizedExpressions.fieldRef(0),
                            VectorizedExpressions.fieldRef(1)
                    )
            );
            try (VectorSchemaRoot projected = VectorizedExpressions.projectBatch(
                    batch, columns, allocator)) {
                assertEquals(3, projected.getRowCount());
                assertEquals(2, projected.getFieldVectors().size());
                IntVector sum = (IntVector) projected.getVector(0);
                IntVector prod = (IntVector) projected.getVector(1);
                assertEquals(11, sum.get(0)); assertEquals(10, prod.get(0));
                assertEquals(22, sum.get(1)); assertEquals(40, prod.get(1));
                assertEquals(33, sum.get(2)); assertEquals(90, prod.get(2));
            }
        }
    }

    // ── Complex expression tree ──

    @Test
    void complexExpressionTree() {
        // (a + b) * 2 > 50
        try (VectorSchemaRoot batch = createTwoIntBatch(
                new int[]{5, 10, 15, 20, 25},
                new int[]{5, 10, 15, 20, 25})) {
            VectorizedEvaluator pred = VectorizedExpressions.gt(
                    VectorizedExpressions.multiply(
                            VectorizedExpressions.add(
                                    VectorizedExpressions.fieldRef(0),
                                    VectorizedExpressions.fieldRef(1)
                            ),
                            VectorizedExpressions.intLiteral(2)
                    ),
                    VectorizedExpressions.intLiteral(50)
            );
            try (FieldVector result = pred.evaluate(batch, allocator)) {
                BitVector bits = (BitVector) result;
                assertEquals(5, bits.getValueCount());
                assertEquals(0, bits.get(0)); // (5+5)*2=20 > 50? no
                assertEquals(0, bits.get(1)); // (10+10)*2=40 > 50? no
                assertEquals(1, bits.get(2)); // (15+15)*2=60 > 50? yes
                assertEquals(1, bits.get(3)); // (20+20)*2=80 > 50? yes
                assertEquals(1, bits.get(4)); // (25+25)*2=100 > 50? yes
            }
        }
    }

    // ── Null handling ──

    @Test
    void nullHandlingInArithmetic() {
        Schema schema = new Schema(List.of(
                new Field("a", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("b", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));
        try (VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator)) {
            batch.allocateNew();
            IntVector a = (IntVector) batch.getVector(0);
            IntVector b = (IntVector) batch.getVector(1);
            a.set(0, 10); b.set(0, 20);
            a.set(1, 30); b.setNull(1);
            a.setNull(2); b.set(2, 40);
            batch.setRowCount(3);

            VectorizedEvaluator eval = VectorizedExpressions.add(
                    VectorizedExpressions.fieldRef(0),
                    VectorizedExpressions.fieldRef(1)
            );
            try (FieldVector result = eval.evaluate(batch, allocator)) {
                IntVector iv = (IntVector) result;
                assertEquals(30, iv.get(0));
                assertTrue(iv.isNull(1));
                assertTrue(iv.isNull(2));
            }
        }
    }

    // ── Double precision ──

    @Test
    void doublePrecisionArithmetic() {
        Schema schema = new Schema(List.of(
                new Field("x", FieldType.nullable(new ArrowType.FloatingPoint(
                        org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)), null)
        ));
        try (VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator)) {
            batch.allocateNew();
            Float8Vector v = (Float8Vector) batch.getVector(0);
            v.set(0, 1.5);
            v.set(1, 2.5);
            v.set(2, 3.5);
            batch.setRowCount(3);

            VectorizedEvaluator eval = VectorizedExpressions.multiply(
                    VectorizedExpressions.fieldRef(0),
                    VectorizedExpressions.doubleLiteral(2.0)
            );
            try (FieldVector result = eval.evaluate(batch, allocator)) {
                Float8Vector rv = (Float8Vector) result;
                assertEquals(3.0, rv.get(0), 0.001);
                assertEquals(5.0, rv.get(1), 0.001);
                assertEquals(7.0, rv.get(2), 0.001);
            }
        }
    }

    // ── String comparison ──

    @Test
    void stringEqualComparison() {
        try (VectorSchemaRoot batch = createStringBatch("apple", "banana", "cherry", "banana")) {
            VectorizedEvaluator pred = VectorizedExpressions.equal(
                    VectorizedExpressions.fieldRef(0),
                    VectorizedExpressions.stringLiteral("banana")
            );
            try (FieldVector result = pred.evaluate(batch, allocator)) {
                BitVector bits = (BitVector) result;
                assertEquals(0, bits.get(0));
                assertEquals(1, bits.get(1));
                assertEquals(0, bits.get(2));
                assertEquals(1, bits.get(3));
            }
        }
    }
}
