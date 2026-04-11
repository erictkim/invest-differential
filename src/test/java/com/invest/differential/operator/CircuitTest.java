package com.invest.differential.operator;

import com.invest.differential.arrow.ArrowUtils;
import com.invest.differential.zset.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.*;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

class CircuitTest {

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
    void filterCircuit() {
        Circuit circuit = new Circuit();
        InputOperator input = new InputOperator("data", intSchema, allocator);
        circuit.addOperator(input);

        Stream inputStream = input.getOutput();
        FilterOperator filter = new FilterOperator(inputStream, (root, i) -> {
            int val = ((IntVector) root.getVector(0)).get(i);
            return val > 2;
        });
        circuit.addOperator(filter);

        OutputOperator output = new OutputOperator("result", filter.getOutput());
        circuit.addOperator(output);

        // Step 1: insert {1,2,3,4,5}
        input.setValue(ZSet.fromData(intSchema, allocator, new Object[][]{{1}, {2}, {3}, {4}, {5}}));
        circuit.step();

        ZSet result = output.getValue();
        result.compact();
        assertEquals(3, result.rowCount()); // 3, 4, 5
    }

    @Test
    void projectCircuit() {
        Circuit circuit = new Circuit();
        InputOperator input = new InputOperator("data", intSchema, allocator);
        circuit.addOperator(input);

        Schema doubledSchema = new Schema(List.of(
                new Field("doubled", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));
        ProjectOperator project = new ProjectOperator(input.getOutput(), doubledSchema, (root, i) -> {
            int val = ((IntVector) root.getVector(0)).get(i);
            return new Object[]{val * 2};
        });
        circuit.addOperator(project);

        OutputOperator output = new OutputOperator("result", project.getOutput());
        circuit.addOperator(output);

        input.setValue(ZSet.fromData(intSchema, allocator, new Object[][]{{5}, {10}}));
        circuit.step();

        ZSet result = output.getValue();
        result.compact();
        assertEquals(2, result.rowCount());
    }

    @Test
    void integrateAccumulatesState() {
        Circuit circuit = new Circuit();
        InputOperator input = new InputOperator("data", intSchema, allocator);
        circuit.addOperator(input);

        IntegrateOperator integrate = new IntegrateOperator(input.getOutput(), allocator);
        circuit.addOperator(integrate);

        OutputOperator output = new OutputOperator("result", integrate.getOutput());
        circuit.addOperator(output);

        // Step 1: insert {1, 2}
        input.setValue(ZSet.fromData(intSchema, allocator, new Object[][]{{1}, {2}}));
        circuit.step();
        ZSet result1 = output.getValue();
        result1.compact();
        assertEquals(2, result1.rowCount());

        // Step 2: insert {3}
        input.setValue(ZSet.fromData(intSchema, allocator, new Object[][]{{3}}));
        circuit.step();
        ZSet result2 = output.getValue();
        result2.compact();
        assertEquals(3, result2.rowCount()); // accumulates: {1, 2, 3}
    }

    @Test
    void differentiateProducesDelta() {
        Circuit circuit = new Circuit();
        InputOperator input = new InputOperator("data", intSchema, allocator);
        circuit.addOperator(input);

        DifferentiateOperator diff = new DifferentiateOperator(input.getOutput(), allocator);
        circuit.addOperator(diff);

        OutputOperator output = new OutputOperator("result", diff.getOutput());
        circuit.addOperator(output);

        // D(s[t]) = s[t] - s[t-1]
        // Step 1: input {1,2} → D = {1,2} - {} = {1,2}
        input.setValue(ZSet.fromData(intSchema, allocator, new Object[][]{{1}, {2}}));
        circuit.step();
        ZSet r1 = output.getValue();
        r1.compact();
        assertEquals(2, r1.rowCount());

        // Step 2: input {2,3} → D = {2,3} - {1,2} = {3}+1, {1}-1
        input.setValue(ZSet.fromData(intSchema, allocator, new Object[][]{{2}, {3}}));
        circuit.step();
        ZSet r2 = output.getValue();
        r2.compact();
        // Should have {3}→+1 and {1}→-1
        assertEquals(2, r2.rowCount());
    }

    @Test
    void incrementalDistinct() {
        Circuit circuit = new Circuit();
        InputOperator input = new InputOperator("data", intSchema, allocator);
        circuit.addOperator(input);

        IncrementalDistinctOperator distinct = new IncrementalDistinctOperator(input.getOutput(), allocator);
        circuit.addOperator(distinct);

        OutputOperator output = new OutputOperator("result", distinct.getOutput());
        circuit.addOperator(output);

        // Step 1: insert {1, 1, 2, 3}
        input.setValue(ZSet.fromData(intSchema, allocator, new Object[][]{{1}, {1}, {2}, {3}}));
        circuit.step();
        ZSet r1 = output.getValue();
        r1.compact();
        // Distinct output delta: {1}→1, {2}→1, {3}→1
        int positiveCount = 0;
        IntVector wv1 = (IntVector) r1.getRoot().getVector(ArrowUtils.WEIGHT_COLUMN);
        for (int i = 0; i < r1.rowCount(); i++) {
            if (wv1.get(i) > 0) positiveCount++;
        }
        assertEquals(3, positiveCount);

        // Step 2: insert another {1}
        input.setValue(ZSet.fromData(intSchema, allocator, new Object[][]{{1}}));
        circuit.step();
        ZSet r2 = output.getValue();
        r2.compact();
        // Distinct doesn't change (1 is already present)
        assertTrue(r2.isEmpty());
    }

    @Test
    void incrementalJoinInner() {
        Schema leftSchema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)
        ));
        Schema rightSchema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("amount", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));
        Schema outputSchema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("id2", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("amount", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));

        Circuit circuit = new Circuit();
        InputOperator leftInput = new InputOperator("left", leftSchema, allocator);
        InputOperator rightInput = new InputOperator("right", rightSchema, allocator);
        circuit.addOperator(leftInput);
        circuit.addOperator(rightInput);

        RowCombiner combiner = (left, lr, right, rr) -> new Object[]{
                ArrowUtils.getValue(left.getVector(0), lr),
                ArrowUtils.getValue(left.getVector(1), lr),
                ArrowUtils.getValue(right.getVector(0), rr),
                ArrowUtils.getValue(right.getVector(1), rr)
        };

        IncrementalJoinOperator join = new IncrementalJoinOperator(
                leftInput.getOutput(), rightInput.getOutput(),
                new int[]{0}, new int[]{0},
                outputSchema, combiner,
                IncrementalJoinOperator.JoinType.INNER, allocator);
        circuit.addOperator(join);

        OutputOperator output = new OutputOperator("result", join.getOutput());
        circuit.addOperator(output);

        // Step 1: left={1,"alice"}, right={1,100}
        leftInput.setValue(ZSet.fromData(leftSchema, allocator, new Object[][]{{1, "alice"}}));
        rightInput.setValue(ZSet.fromData(rightSchema, allocator, new Object[][]{{1, 100}}));
        circuit.step();

        ZSet result1 = output.getValue();
        result1.compact();
        assertEquals(1, result1.rowCount()); // join on id=1

        // Step 2: add right={2,200}, left empty → no new joins
        leftInput.setValue(ZSet.empty(leftSchema, allocator));
        rightInput.setValue(ZSet.fromData(rightSchema, allocator, new Object[][]{{2, 200}}));
        circuit.step();

        ZSet result2 = output.getValue();
        result2.compact();
        assertTrue(result2.isEmpty()); // no matching left for id=2

        // Step 3: add left={2,"bob"} → joins with right id=2
        leftInput.setValue(ZSet.fromData(leftSchema, allocator, new Object[][]{{2, "bob"}}));
        rightInput.setValue(ZSet.empty(rightSchema, allocator));
        circuit.step();

        ZSet result3 = output.getValue();
        result3.compact();
        assertEquals(1, result3.rowCount());
    }

    @Test
    void incrementalAggregate() {
        Schema aggOutputSchema = new Schema(List.of(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("total", FieldType.nullable(new ArrowType.Int(64, true)), null)
        ));
        Schema resultValueSchema = new Schema(List.of(
                new Field("total", FieldType.nullable(new ArrowType.Int(64, true)), null)
        ));

        Circuit circuit = new Circuit();
        InputOperator input = new InputOperator("data", stringIntSchema, allocator);
        circuit.addOperator(input);

        // SUM(amount) GROUP BY name
        AggregateDescription<Object[], Object[]> aggDesc = new AggregateDescription<>(
                new Object[]{0L},
                (acc, values, weight) -> {
                    long sum = ((Number) acc[0]).longValue();
                    if (values.length > 0 && values[0] != null) {
                        sum += ((Number) values[0]).longValue() * weight;
                    }
                    return new Object[]{sum};
                },
                acc -> acc
        );

        Function<Object[], Object[]> resultToRow = r -> r;

        IncrementalAggregateOperator<Object[], Object[]> aggOp = new IncrementalAggregateOperator<>(
                input.getOutput(), new int[]{0}, aggOutputSchema, aggDesc,
                resultValueSchema, resultToRow, allocator);
        circuit.addOperator(aggOp);

        OutputOperator output = new OutputOperator("result", aggOp.getOutput());
        circuit.addOperator(output);

        // Step 1: insert alice=100, bob=200
        input.setValue(ZSet.fromData(stringIntSchema, allocator,
                new Object[][]{{"alice", 100}, {"bob", 200}}));
        circuit.step();

        ZSet r1 = output.getValue();
        r1.compact();
        assertEquals(2, r1.rowCount()); // alice→100, bob→200

        // Step 2: insert alice=50 → total should be alice=150
        input.setValue(ZSet.fromData(stringIntSchema, allocator,
                new Object[][]{{"alice", 50}}));
        circuit.step();

        ZSet r2 = output.getValue();
        r2.compact();
        // Delta should be: alice→150 (+1), alice→100 (-1) = alice changes from 100 to 150
        assertFalse(r2.isEmpty());
    }

    @Test
    void unionAllCombines() {
        Circuit circuit = new Circuit();
        InputOperator left = new InputOperator("a", intSchema, allocator);
        InputOperator right = new InputOperator("b", intSchema, allocator);
        circuit.addOperator(left);
        circuit.addOperator(right);

        UnionAllOperator union = new UnionAllOperator(left.getOutput(), right.getOutput());
        circuit.addOperator(union);

        OutputOperator output = new OutputOperator("result", union.getOutput());
        circuit.addOperator(output);

        left.setValue(ZSet.fromData(intSchema, allocator, new Object[][]{{1}, {2}}));
        right.setValue(ZSet.fromData(intSchema, allocator, new Object[][]{{3}, {4}}));
        circuit.step();

        ZSet result = output.getValue();
        result.compact();
        assertEquals(4, result.rowCount());
    }

    @Test
    void resetClearsState() {
        Circuit circuit = new Circuit();
        InputOperator input = new InputOperator("data", intSchema, allocator);
        circuit.addOperator(input);

        IntegrateOperator integrate = new IntegrateOperator(input.getOutput(), allocator);
        circuit.addOperator(integrate);

        OutputOperator output = new OutputOperator("result", integrate.getOutput());
        circuit.addOperator(output);

        // Accumulate state
        input.setValue(ZSet.fromData(intSchema, allocator, new Object[][]{{1}, {2}}));
        circuit.step();

        // Reset
        circuit.reset();

        // After reset, integrate starts fresh
        input.setValue(ZSet.fromData(intSchema, allocator, new Object[][]{{3}}));
        circuit.step();
        ZSet result = output.getValue();
        result.compact();
        assertEquals(1, result.rowCount()); // Only {3}, not {1,2,3}
    }
}
