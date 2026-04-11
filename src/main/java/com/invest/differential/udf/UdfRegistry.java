package com.invest.differential.udf;

import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.expression.FunctionMappings;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.type.*;
import org.apache.calcite.sql.util.SqlOperatorTables;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for user-defined scalar and aggregate functions.
 *
 * <p>Registered UDFs/UDAFs are available in both SQL queries (via Calcite) and
 * Substrait plans (via custom extensions).
 */
public final class UdfRegistry {

    private static final String UDF_EXTENSION_URI = "/functions_udf.yaml";

    private final Map<String, UdfEntry> udfs = new ConcurrentHashMap<>();
    private final Map<String, UdafEntry> udafs = new ConcurrentHashMap<>();

    public static final class UdfEntry {
        private final String name;
        private final ScalarUdf implementation;
        private final String[] argTypes;   // Substrait type strings: "string", "i32", "i64", "fp64", "boolean"
        private final String returnType;   // Substrait type string
        private final SqlFunction sqlFunction;

        UdfEntry(String name, ScalarUdf implementation, String[] argTypes, String returnType, SqlFunction sqlFunction) {
            this.name = name;
            this.implementation = implementation;
            this.argTypes = argTypes;
            this.returnType = returnType;
            this.sqlFunction = sqlFunction;
        }

        public String name() { return name; }
        public ScalarUdf implementation() { return implementation; }
        public String[] argTypes() { return argTypes; }
        public String returnType() { return returnType; }
        public SqlFunction sqlFunction() { return sqlFunction; }
    }

    /**
     * Register a scalar UDF.
     *
     * @param name       function name (case-insensitive in SQL)
     * @param impl       function implementation
     * @param argTypes   Substrait type names for each argument ("string", "i32", "i64", "fp64", "boolean")
     * @param returnType Substrait type name for the return value
     */
    public UdfRegistry register(String name, ScalarUdf impl, String[] argTypes, String returnType) {
        String lowerName = name.toLowerCase(Locale.ROOT);
        SqlFunction sqlFunc = createCalciteSqlFunction(name, argTypes, returnType);
        udfs.put(lowerName, new UdfEntry(lowerName, impl, argTypes, returnType, sqlFunc));
        return this;
    }

    public static final class UdafEntry {
        private final String name;
        private final AggregateUdf implementation;
        private final String argType;
        private final String returnType;
        private final SqlAggFunction sqlAggFunction;

        UdafEntry(String name, AggregateUdf implementation, String argType, String returnType, SqlAggFunction sqlAggFunction) {
            this.name = name;
            this.implementation = implementation;
            this.argType = argType;
            this.returnType = returnType;
            this.sqlAggFunction = sqlAggFunction;
        }

        public String name() { return name; }
        public AggregateUdf implementation() { return implementation; }
        public String argType() { return argType; }
        public String returnType() { return returnType; }
        public SqlAggFunction sqlAggFunction() { return sqlAggFunction; }
    }

    /**
     * Register an aggregate UDF (UDAF).
     *
     * @param name       function name (case-insensitive in SQL)
     * @param impl       aggregate function implementation
     * @param argType    Substrait type name for the argument ("string", "i32", "i64", "fp64", "boolean")
     * @param returnType Substrait type name for the return value
     */
    public UdfRegistry registerUdaf(String name, AggregateUdf impl, String argType, String returnType) {
        String lowerName = name.toLowerCase(Locale.ROOT);
        SqlAggFunction sqlAggFunc = createCalciteSqlAggFunction(name, argType, returnType);
        udafs.put(lowerName, new UdafEntry(lowerName, impl, argType, returnType, sqlAggFunc));
        return this;
    }

    public boolean isEmpty() {
        return udfs.isEmpty() && udafs.isEmpty();
    }

    public UdfEntry get(String name) {
        return udfs.get(name.toLowerCase(Locale.ROOT));
    }

    public UdafEntry getUdaf(String name) {
        return udafs.get(name.toLowerCase(Locale.ROOT));
    }

    public Collection<UdfEntry> all() {
        return Collections.unmodifiableCollection(udfs.values());
    }

    public Collection<UdafEntry> allUdafs() {
        return Collections.unmodifiableCollection(udafs.values());
    }

    /**
     * Build a Calcite SqlOperatorTable containing all registered UDFs.
     */
    public SqlOperatorTable buildOperatorTable() {
        List<SqlOperator> ops = new ArrayList<>();
        for (UdfEntry entry : udfs.values()) {
            ops.add(entry.sqlFunction());
        }
        for (UdafEntry entry : udafs.values()) {
            ops.add(entry.sqlAggFunction());
        }
        return SqlOperatorTables.of(ops);
    }

    /**
     * Build FunctionMappings.Sig entries for all registered UDFs.
     */
    public List<FunctionMappings.Sig> buildSigs() {
        List<FunctionMappings.Sig> sigs = new ArrayList<>();
        for (UdfEntry entry : udfs.values()) {
            sigs.add(FunctionMappings.s(entry.sqlFunction(), entry.name()));
        }
        return sigs;
    }

    public List<FunctionMappings.Sig> buildAggSigs() {
        List<FunctionMappings.Sig> sigs = new ArrayList<>();
        for (UdafEntry entry : udafs.values()) {
            sigs.add(FunctionMappings.s(entry.sqlAggFunction(), entry.name()));
        }
        return sigs;
    }

    /**
     * Load a Substrait ExtensionCollection containing all registered UDFs,
     * merged with the default Substrait extensions.
     */
    public SimpleExtension.ExtensionCollection buildMergedExtensions() {
        try {
            SimpleExtension.ExtensionCollection defaults = SimpleExtension.loadDefaults();
            if (udfs.isEmpty() && udafs.isEmpty()) {
                return defaults;
            }
            String yaml = buildExtensionYaml();
            SimpleExtension.ExtensionCollection udfExt = SimpleExtension.load(
                    UDF_EXTENSION_URI,
                    new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)));
            return defaults.merge(udfExt);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load Substrait extensions for UDFs", e);
        }
    }

    private String buildExtensionYaml() {
        StringBuilder sb = new StringBuilder();
        sb.append("%YAML 1.2\n---\n");
        if (!udfs.isEmpty()) {
            sb.append("scalar_functions:\n");
            for (UdfEntry entry : udfs.values()) {
                sb.append("  -\n");
                sb.append("    name: \"").append(entry.name()).append("\"\n");
                sb.append("    impls:\n");
                sb.append("      - args:\n");
                for (String argType : entry.argTypes()) {
                    sb.append("          - value: ").append(argType).append("\n");
                }
                sb.append("        return: ").append(entry.returnType()).append("\n");
            }
        }
        if (!udafs.isEmpty()) {
            sb.append("aggregate_functions:\n");
            for (UdafEntry entry : udafs.values()) {
                sb.append("  -\n");
                sb.append("    name: \"").append(entry.name()).append("\"\n");
                sb.append("    impls:\n");
                sb.append("      - args:\n");
                sb.append("          - name: x\n");
                sb.append("            value: ").append(entry.argType()).append("\n");
                sb.append("        decomposable: NONE\n");
                sb.append("        intermediate: ").append(entry.returnType()).append("\n");
                sb.append("        return: ").append(entry.returnType()).append("\n");
            }
        }
        return sb.toString();
    }

    private static SqlAggFunction createCalciteSqlAggFunction(String name, String argType, String returnType) {
        SqlReturnTypeInference returnTypeInference = toReturnTypeInference(returnType);
        SqlOperandTypeChecker operandTypeChecker = toOperandTypeChecker(new String[]{argType});

        return new SqlAggFunction(
                name.toUpperCase(Locale.ROOT),
                null,
                SqlKind.OTHER_FUNCTION,
                returnTypeInference,
                null,
                operandTypeChecker,
                SqlFunctionCategory.USER_DEFINED_FUNCTION,
                false,
                false,
                org.apache.calcite.util.Optionality.FORBIDDEN) {
        };
    }

    private static SqlFunction createCalciteSqlFunction(String name, String[] argTypes, String returnType) {
        SqlReturnTypeInference returnTypeInference = toReturnTypeInference(returnType);
        SqlOperandTypeChecker operandTypeChecker = toOperandTypeChecker(argTypes);

        return new SqlFunction(
                name.toUpperCase(Locale.ROOT),
                SqlKind.OTHER_FUNCTION,
                returnTypeInference,
                null,
                operandTypeChecker,
                SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    private static SqlReturnTypeInference toReturnTypeInference(String substraitType) {
        return switch (substraitType) {
            case "string" -> ReturnTypes.VARCHAR_2000;
            case "i32" -> ReturnTypes.INTEGER;
            case "i64" -> ReturnTypes.BIGINT;
            case "fp32", "fp64" -> ReturnTypes.DOUBLE;
            case "boolean" -> ReturnTypes.BOOLEAN;
            default -> ReturnTypes.VARCHAR_2000;
        };
    }

    private static SqlOperandTypeChecker toOperandTypeChecker(String[] argTypes) {
        SqlTypeFamily[] families = new SqlTypeFamily[argTypes.length];
        for (int i = 0; i < argTypes.length; i++) {
            families[i] = toTypeFamily(argTypes[i]);
        }
        return OperandTypes.family(families);
    }

    private static SqlTypeFamily toTypeFamily(String substraitType) {
        return switch (substraitType) {
            case "string" -> SqlTypeFamily.STRING;
            case "i32", "i64" -> SqlTypeFamily.INTEGER;
            case "fp32", "fp64" -> SqlTypeFamily.NUMERIC;
            case "boolean" -> SqlTypeFamily.BOOLEAN;
            default -> SqlTypeFamily.ANY;
        };
    }
}
