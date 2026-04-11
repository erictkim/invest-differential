package io.substrait.isthmus;

import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.calcite.SubstraitOperatorTable;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.FunctionMappings;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import java.util.List;

/**
 * Extends the SQL-to-Substrait pipeline to support user-defined functions (UDFs).
 *
 * <p>This class lives in the {@code io.substrait.isthmus} package to access
 * package-private members of the Isthmus library (e.g., {@code SqlConverterBase},
 * {@code DefinedTable}, etc.).
 */
public class UdfSqlToSubstrait extends SqlConverterBase {

    private final SimpleExtension.ExtensionCollection mergedExtensions;
    private final List<FunctionMappings.Sig> additionalSigs;
    private final List<FunctionMappings.Sig> additionalAggSigs;
    private final SqlOperatorTable udfOperatorTable;

    /**
     * @param udfOperatorTable  Calcite operator table containing UDF/UDAF functions
     * @param additionalSigs    FunctionMappings.Sig entries mapping scalar UDF SqlFunctions to Substrait names
     * @param mergedExtensions  Substrait extension collection including both default + UDF/UDAF functions
     */
    public UdfSqlToSubstrait(
            SqlOperatorTable udfOperatorTable,
            List<FunctionMappings.Sig> additionalSigs,
            SimpleExtension.ExtensionCollection mergedExtensions) {
        this(udfOperatorTable, additionalSigs, List.of(), mergedExtensions);
    }

    /**
     * @param udfOperatorTable   Calcite operator table containing UDF/UDAF functions
     * @param additionalSigs     FunctionMappings.Sig entries mapping scalar UDF SqlFunctions to Substrait names
     * @param additionalAggSigs  FunctionMappings.Sig entries mapping UDAF SqlAggFunctions to Substrait names
     * @param mergedExtensions   Substrait extension collection including both default + UDF/UDAF functions
     */
    public UdfSqlToSubstrait(
            SqlOperatorTable udfOperatorTable,
            List<FunctionMappings.Sig> additionalSigs,
            List<FunctionMappings.Sig> additionalAggSigs,
            SimpleExtension.ExtensionCollection mergedExtensions) {
        super(null);
        this.udfOperatorTable = udfOperatorTable;
        this.additionalSigs = additionalSigs;
        this.additionalAggSigs = additionalAggSigs;
        this.mergedExtensions = mergedExtensions;
    }

    /**
     * Convert a SQL query to a Substrait protobuf Plan, with UDF support.
     */
    public Plan execute(String sql, List<String> tables) throws SqlParseException {
        // Step 1: Build the Calcite schema from CREATE TABLE statements
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
        CalciteCatalogReader catalogReader =
                new CalciteCatalogReader(rootSchema, List.of(), factory, config);

        // Use standard validator for parsing CREATE TABLE statements
        SqlValidator createTableValidator = Validator.create(factory, catalogReader, SqlValidator.Config.DEFAULT);
        if (tables != null) {
            for (String tableDef : tables) {
                List<DefinedTable> tList = parseCreateTable(factory, createTableValidator, tableDef);
                for (DefinedTable t : tList) {
                    rootSchema.add(t.getName(), t);
                }
            }
        }

        // Step 2: Create composite operator table: UDFs + standard Substrait operators
        SqlOperatorTable compositeTable = SqlOperatorTables.chain(
                udfOperatorTable, SubstraitOperatorTable.INSTANCE);

        // Step 3: Create a fresh catalog reader after schema is populated,
        //         and a validator that knows about UDFs
        CalciteCatalogReader freshReader =
                new CalciteCatalogReader(rootSchema, List.of(), factory, config);
        SqlValidator queryValidator = new UdfValidator(
                compositeTable, freshReader, factory, SqlValidator.Config.DEFAULT);

        // Step 4: Parse SQL and convert to Calcite RelNode
        SqlParser parser = SqlParser.create(sql, parserConfig);
        var parsedList = parser.parseStmtList();

        SqlToRelConverter converter = new SqlToRelConverter(
                null, queryValidator, freshReader, relOptCluster,
                StandardConvertletTable.INSTANCE, converterConfig.withExpand(true));

        // Step 5: Build the Substrait protobuf Plan
        var plan = Plan.newBuilder();
        ExtensionCollector functionCollector = new ExtensionCollector();
        var relProtoConverter = new RelProtoConverter(functionCollector);

        for (SqlNode parsed : parsedList) {
            RelRoot root = getBestExpRelRoot(converter, parsed);
            Rel substraitRel = convertWithUdfSupport(root.rel);

            plan.addRelations(
                    PlanRel.newBuilder()
                            .setRoot(
                                    io.substrait.proto.RelRoot.newBuilder()
                                            .setInput(substraitRel.accept(relProtoConverter))
                                            .addAllNames(
                                                    TypeConverter.DEFAULT
                                                            .toNamedStruct(root.validatedRowType)
                                                            .names())));
        }

        functionCollector.addExtensionsToPlan(plan);
        return plan.build();
    }

    private Rel convertWithUdfSupport(RelNode rel) {
        RelDataTypeFactory typeFactory = rel.getCluster().getTypeFactory();

        // Create a ScalarFunctionConverter that includes UDF mappings
        ScalarFunctionConverter scalarConverter = new ScalarFunctionConverter(
                mergedExtensions.scalarFunctions(),
                additionalSigs,
                typeFactory,
                TypeConverter.DEFAULT);

        AggregateFunctionConverter aggregateConverter = new AggregateFunctionConverter(
                mergedExtensions.aggregateFunctions(),
                additionalAggSigs,
                typeFactory,
                TypeConverter.DEFAULT);

        WindowFunctionConverter windowConverter = new WindowFunctionConverter(
                mergedExtensions.windowFunctions(), typeFactory);

        SubstraitRelVisitor visitor = new SubstraitRelVisitor(
                typeFactory,
                scalarConverter,
                aggregateConverter,
                windowConverter,
                TypeConverter.DEFAULT,
                featureBoard);

        return visitor.apply(rel);
    }

    private static RelRoot getBestExpRelRoot(SqlToRelConverter converter, SqlNode parsed) {
        RelRoot root = converter.convertQuery(parsed, true, true);
        var program = HepProgram.builder().build();
        HepPlanner hepPlanner = new HepPlanner(program);
        hepPlanner.setRoot(root.rel);
        return root.withRel(hepPlanner.findBestExp());
    }

    /**
     * Simple SqlValidator subclass that accepts a custom operator table.
     */
    private static class UdfValidator extends SqlValidatorImpl {
        UdfValidator(SqlOperatorTable opTab,
                     org.apache.calcite.sql.validate.SqlValidatorCatalogReader catalogReader,
                     RelDataTypeFactory typeFactory,
                     Config config) {
            super(opTab, catalogReader, typeFactory, config);
        }
    }
}
