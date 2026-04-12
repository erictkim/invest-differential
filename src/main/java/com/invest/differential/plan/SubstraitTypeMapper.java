package com.invest.differential.plan;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import io.substrait.type.Type;

/**
 * Maps between Substrait types and Arrow types.
 */
public final class SubstraitTypeMapper {

    private SubstraitTypeMapper() {}

    public static ArrowType toArrowType(Type substraitType) {
        if (substraitType instanceof Type.Bool) {
            return new ArrowType.Bool();
        } else if (substraitType instanceof Type.I8) {
            return new ArrowType.Int(8, true);
        } else if (substraitType instanceof Type.I16) {
            return new ArrowType.Int(16, true);
        } else if (substraitType instanceof Type.I32) {
            return new ArrowType.Int(32, true);
        } else if (substraitType instanceof Type.I64) {
            return new ArrowType.Int(64, true);
        } else if (substraitType instanceof Type.FP32) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        } else if (substraitType instanceof Type.FP64) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        } else if (substraitType instanceof Type.Str) {
            return new ArrowType.Utf8();
        } else if (substraitType instanceof Type.VarChar) {
            return new ArrowType.Utf8();
        } else if (substraitType instanceof Type.FixedChar) {
            return new ArrowType.Utf8();
        } else if (substraitType instanceof Type.Binary) {
            return new ArrowType.Binary();
        } else if (substraitType instanceof Type.Date) {
            return new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY);
        } else if (substraitType instanceof Type.Timestamp) {
            return new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, null);
        } else if (substraitType instanceof Type.TimestampTZ) {
            return new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, "UTC");
        } else if (substraitType instanceof Type.PrecisionTimestamp pt) {
            return new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, null);
        } else if (substraitType instanceof Type.PrecisionTimestampTZ pt) {
            return new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, "UTC");
        } else if (substraitType instanceof Type.Decimal dec) {
            return new ArrowType.Decimal(dec.precision(), dec.scale(), 128);
        }
        throw new UnsupportedOperationException("Unsupported Substrait type: " + substraitType);
    }

    public static boolean isNullable(Type substraitType) {
        return substraitType.nullable();
    }

    public static Field toArrowField(String name, Type substraitType) {
        ArrowType arrowType = toArrowType(substraitType);
        boolean nullable = isNullable(substraitType);
        FieldType fieldType = nullable ? FieldType.nullable(arrowType) : FieldType.notNullable(arrowType);
        return new Field(name, fieldType, null);
    }
}
