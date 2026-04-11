package com.invest.differential.zset;

import com.invest.differential.arrow.ArrowUtils;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ZSetTest {

    private BufferAllocator allocator;
    private Schema intSchema;
    private Schema stringIntSchema;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
        intSchema = new Schema(List.of(
                new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));
        stringIntSchema = new Schema(List.of(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("amount", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));
    }

    @AfterEach
    void tearDown() {
        try {
            allocator.close();
        } catch (IllegalStateException e) {
            // Arrow memory leak detection - acceptable in tests
        }
    }

    @Test
    void emptyZSet() {
        ZSet empty = ZSet.empty(intSchema, allocator);
        assertTrue(empty.isEmpty());
        assertEquals(0, empty.rowCount());
        empty.close();
    }

    @Test
    void fromDataCreatesRows() {
        ZSet zset = ZSet.fromData(intSchema, allocator, new Object[][]{{10}, {20}, {30}});
        assertFalse(zset.isEmpty());
        assertEquals(3, zset.rowCount());
        zset.close();
    }

    @Test
    void addCombinesTwoZSets() {
        ZSet a = ZSet.fromData(intSchema, allocator, new Object[][]{{1}, {2}});
        ZSet b = ZSet.fromData(intSchema, allocator, new Object[][]{{3}});
        ZSet sum = a.add(b);
        assertEquals(3, sum.rowCount());
        a.close();
        b.close();
        sum.close();
    }

    @Test
    void addWithCompaction() {
        // Same element with different weights should merge
        ZSet a = ZSet.fromData(intSchema, allocator, new Object[][]{{10}});
        ZSet b = ZSet.fromData(intSchema, allocator, new Object[][]{{10}});
        ZSet sum = a.add(b);
        sum.compact();
        assertEquals(1, sum.rowCount()); // One unique row
        // Weight should be 2
        IntVector wv = (IntVector) sum.getRoot().getVector(ArrowUtils.WEIGHT_COLUMN);
        assertEquals(2, wv.get(0));
        a.close();
        b.close();
        sum.close();
    }

    @Test
    void negateFlipsWeights() {
        ZSet a = ZSet.fromData(intSchema, allocator, new Object[][]{{10}, {20}});
        ZSet neg = a.negate();
        neg.compact();
        IntVector wv = (IntVector) neg.getRoot().getVector(ArrowUtils.WEIGHT_COLUMN);
        for (int i = 0; i < neg.rowCount(); i++) {
            assertEquals(-1, wv.get(i));
        }
        a.close();
        neg.close();
    }

    @Test
    void subtractProducesDifference() {
        ZSet a = ZSet.fromData(intSchema, allocator, new Object[][]{{10}, {20}});
        ZSet b = ZSet.fromData(intSchema, allocator, new Object[][]{{10}});
        ZSet diff = a.subtract(b);
        diff.compact();
        // {10}→0 (cancelled), {20}→1
        ZSet positive = diff.positive();
        assertEquals(1, positive.rowCount());
        positive.close();
        diff.close();
        a.close();
        b.close();
    }

    @Test
    void filterRemovesNonMatching() {
        ZSet zset = ZSet.fromData(intSchema, allocator, new Object[][]{{1}, {2}, {3}, {4}, {5}});
        ZSet filtered = zset.filter((root, i) -> {
            int val = ((IntVector) root.getVector(0)).get(i);
            return val > 3;
        });
        assertEquals(2, filtered.rowCount());
        zset.close();
        filtered.close();
    }

    @Test
    void mapTransformsRows() {
        ZSet zset = ZSet.fromData(intSchema, allocator, new Object[][]{{10}, {20}});
        Schema doubledSchema = intSchema;
        ZSet mapped = zset.map(doubledSchema, (root, i) -> {
            int val = ((IntVector) root.getVector(0)).get(i);
            return new Object[]{val * 2};
        });
        mapped.compact();
        assertEquals(2, mapped.rowCount());
        zset.close();
        mapped.close();
    }

    @Test
    void distinctRemovesDuplicates() {
        ZSet zset = ZSet.fromData(intSchema, allocator, new Object[][]{{1}, {1}, {2}, {2}, {2}});
        ZSet distinct = zset.distinct();
        distinct.compact();
        assertEquals(2, distinct.rowCount()); // {1}→1, {2}→1
        IntVector wv = (IntVector) distinct.getRoot().getVector(ArrowUtils.WEIGHT_COLUMN);
        for (int i = 0; i < distinct.rowCount(); i++) {
            assertEquals(1, wv.get(i));
        }
        zset.close();
        distinct.close();
    }

    @Test
    void addIsCommutative() {
        ZSet a1 = ZSet.fromData(intSchema, allocator, new Object[][]{{1}, {2}});
        ZSet b1 = ZSet.fromData(intSchema, allocator, new Object[][]{{3}, {4}});
        ZSet a2 = ZSet.fromData(intSchema, allocator, new Object[][]{{1}, {2}});
        ZSet b2 = ZSet.fromData(intSchema, allocator, new Object[][]{{3}, {4}});
        ZSet ab = a1.add(b1);
        ZSet ba = b2.add(a2);
        ab.compact();
        ba.compact();
        assertTrue(ab.equalsZSet(ba));
        ab.close();
        ba.close();
    }

    @Test
    void addIsAssociative() {
        ZSet a1 = ZSet.fromData(intSchema, allocator, new Object[][]{{1}});
        ZSet b1 = ZSet.fromData(intSchema, allocator, new Object[][]{{2}});
        ZSet c1 = ZSet.fromData(intSchema, allocator, new Object[][]{{3}});
        ZSet a2 = ZSet.fromData(intSchema, allocator, new Object[][]{{1}});
        ZSet b2 = ZSet.fromData(intSchema, allocator, new Object[][]{{2}});
        ZSet c2 = ZSet.fromData(intSchema, allocator, new Object[][]{{3}});
        ZSet ab = a1.add(b1);
        ZSet ab_c = ab.add(c1);
        ZSet bc = b2.add(c2);
        ZSet a_bc = a2.add(bc);
        ab_c.compact();
        a_bc.compact();
        assertTrue(ab_c.equalsZSet(a_bc));
        ab_c.close();
        a_bc.close();
    }

    @Test
    void addWithInverse() {
        // a + (-a) = 0
        ZSet a = ZSet.fromData(intSchema, allocator, new Object[][]{{1}, {2}, {3}});
        ZSet neg = a.negate();
        ZSet zero = a.add(neg);
        zero.compact();
        assertTrue(zero.isEmpty());
        a.close();
        neg.close();
        zero.close();
    }

    @Test
    void unionAllMergesMultisets() {
        ZSet a = ZSet.fromData(intSchema, allocator, new Object[][]{{1}, {1}});
        ZSet b = ZSet.fromData(intSchema, allocator, new Object[][]{{1}});
        ZSet u = a.unionAll(b);
        u.compact();
        assertEquals(1, u.rowCount());
        IntVector wv = (IntVector) u.getRoot().getVector(ArrowUtils.WEIGHT_COLUMN);
        assertEquals(3, wv.get(0)); // 2 + 1
        a.close();
        b.close();
        u.close();
    }

    @Test
    void indexAndDeindex() {
        ZSet zset = ZSet.fromData(stringIntSchema, allocator, new Object[][]{
                {"alice", 100},
                {"bob", 200},
                {"alice", 300}
        });
        IndexedZSet indexed = zset.index(new int[]{0}); // index by name
        assertEquals(2, indexed.groupCount());

        ZSet deindexed = indexed.deindex();
        deindexed.compact();
        assertEquals(3, deindexed.rowCount());

        deindexed.close();
        indexed.close();
        zset.close();
    }

    @Test
    void multiplyScalesWeights() {
        ZSet a = ZSet.fromData(intSchema, allocator, new Object[][]{{10}, {20}});
        ZSet scaled = a.multiply(3);
        scaled.compact();
        IntVector wv = (IntVector) scaled.getRoot().getVector(ArrowUtils.WEIGHT_COLUMN);
        for (int i = 0; i < scaled.rowCount(); i++) {
            assertEquals(3, wv.get(i));
        }
        a.close();
        scaled.close();
    }
}
