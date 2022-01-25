package jp.co.yahoo.yosegi.spark.inmemory;

import jp.co.yahoo.yosegi.inmemory.ILoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.SparkArrayLoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.SparkMapLoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.SparkStructLoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.map.SparkMapChildBooleanLoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.map.SparkMapChildByteLoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.map.SparkMapChildBytesLoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.map.SparkMapChildDecimalLoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.map.SparkMapChildDoubleLoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.map.SparkMapChildFloatLoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.map.SparkMapChildIntegerLoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.map.SparkMapChildLongLoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.map.SparkMapChildShortLoaderFactory;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;

public class SparkMapChildLoaderFactoryUtil {
  public SparkMapChildLoaderFactoryUtil() {
  }

  public static ILoaderFactory<WritableColumnVector> createLoaderFactory(
      final WritableColumnVector vector, final int totalChildCount, final int currentChildCount) {
    if (vector == null) {
      // TODO: return null loader
    }
    Class klass = vector.dataType().getClass();
    if (klass == ArrayType.class) {
      return new SparkArrayLoaderFactory(vector);
    } else if (klass == StructType.class) {
      return new SparkStructLoaderFactory(vector);
    } else if (klass == DataTypes.StringType.getClass()
        || klass == DataTypes.BinaryType.getClass()) {
      return new SparkMapChildBytesLoaderFactory(vector, totalChildCount, currentChildCount);
    } else if (klass == DataTypes.BooleanType.getClass()) {
      return new SparkMapChildBooleanLoaderFactory(vector, totalChildCount, currentChildCount);
    } else if (klass == DataTypes.ByteType.getClass()) {
      return new SparkMapChildByteLoaderFactory(vector, totalChildCount, currentChildCount);
    } else if (klass == DataTypes.ShortType.getClass()) {
      return new SparkMapChildShortLoaderFactory(vector, totalChildCount, currentChildCount);
    } else if (klass == DataTypes.IntegerType.getClass()) {
      return new SparkMapChildIntegerLoaderFactory(vector, totalChildCount, currentChildCount);
    } else if (klass == DataTypes.LongType.getClass()) {
      return new SparkMapChildLongLoaderFactory(vector, totalChildCount, currentChildCount);
    } else if (klass == DataTypes.FloatType.getClass()) {
      return new SparkMapChildFloatLoaderFactory(vector, totalChildCount, currentChildCount);
    } else if (klass == DataTypes.DoubleType.getClass()) {
      return new SparkMapChildDoubleLoaderFactory(vector, totalChildCount, currentChildCount);
    } else if (klass == DataTypes.TimestampType.getClass()) {
      return new SparkMapChildLongLoaderFactory(vector, totalChildCount, currentChildCount);
    } else if (klass == DecimalType.class) {
      return new SparkMapChildDecimalLoaderFactory(vector, totalChildCount, currentChildCount);
    } else if (klass == MapType.class) {
      if (!(vector.getChild(0).dataType() instanceof StringType)) {
        throw new UnsupportedOperationException(
            makeErrorMessage(vector) + ". Map key type is string only.");
      }
      return new SparkMapLoaderFactory(vector);
    }
    throw new UnsupportedOperationException(makeErrorMessage(vector));
  }

  private static String makeErrorMessage(final WritableColumnVector vector) {
    return "Unsupported datatype : " + vector.dataType().toString();
  }
}
