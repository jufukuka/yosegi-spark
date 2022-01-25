package jp.co.yahoo.yosegi.spark.inmemory;

import jp.co.yahoo.yosegi.inmemory.ILoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.SparkArrayLoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.SparkBooleanLoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.SparkByteLoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.SparkBytesLoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.SparkDecimalLoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.SparkDoubleLoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.SparkFloatLoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.SparkIntegerLoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.SparkLongLoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.SparkMapLoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.SparkShortLoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.SparkStructLoaderFactory;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;

public class SparkLoaderFactoryUtil {
  private SparkLoaderFactoryUtil() {}

  public static ILoaderFactory<WritableColumnVector> createLoaderFactory(
      final WritableColumnVector vector) {
    if (vector == null) {
      // TODO: return null loader
    }
    final Class klass = vector.dataType().getClass();
    if (klass == ArrayType.class) {
      return new SparkArrayLoaderFactory(vector);
    } else if (klass == StructType.class) {
      return new SparkStructLoaderFactory(vector);
    } else if (klass == DataTypes.StringType.getClass()
        || klass == DataTypes.BinaryType.getClass()) {
      return new SparkBytesLoaderFactory(vector);
    } else if (klass == DataTypes.BooleanType.getClass()) {
      return new SparkBooleanLoaderFactory(vector);
    } else if (klass == DataTypes.ByteType.getClass()) {
      return new SparkByteLoaderFactory(vector);
    } else if (klass == DataTypes.ShortType.getClass()) {
      return new SparkShortLoaderFactory(vector);
    } else if (klass == DataTypes.IntegerType.getClass()) {
      return new SparkIntegerLoaderFactory(vector);
    } else if (klass == DataTypes.LongType.getClass()) {
      return new SparkLongLoaderFactory(vector);
    } else if (klass == DataTypes.FloatType.getClass()) {
      return new SparkFloatLoaderFactory(vector);
    } else if (klass == DataTypes.DoubleType.getClass()) {
      return new SparkDoubleLoaderFactory(vector);
    } else if (klass == DataTypes.TimestampType.getClass()) {
      return new SparkLongLoaderFactory(vector);
    } else if (klass == DecimalType.class) {
      return new SparkDecimalLoaderFactory(vector);
    } else if (klass == MapType.class) {
      if (vector.getChild(0).dataType().getClass() != DataTypes.StringType.getClass()) {
        throw new UnsupportedOperationException(
            makeErrorMessage(vector) + ". Map key type is string only.");
      }
      // FIXME: Map type does not support composite type values.
      final Class valueClass = vector.getChild(1).dataType().getClass();
      if (valueClass == ArrayType.class) {
        throw new UnsupportedOperationException(
            makeErrorMessage(vector) + ". Map type does not support array type values.");
      } else if (valueClass == StructType.class) {
        throw new UnsupportedOperationException(
            makeErrorMessage(vector) + ". Map type does not support struct type values.");
      } else if (valueClass == MapType.class) {
        throw new UnsupportedOperationException(
            makeErrorMessage(vector) + ". Map type does not support map type values.");
      }
      return new SparkMapLoaderFactory(vector);
    }
    throw new UnsupportedOperationException(makeErrorMessage(vector));
  }

  private static String makeErrorMessage(final WritableColumnVector vector) {
    return "Unsupported datatype : " + vector.dataType().toString();
  }
}
