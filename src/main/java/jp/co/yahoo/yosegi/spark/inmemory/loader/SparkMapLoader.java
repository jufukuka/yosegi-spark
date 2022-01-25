package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.inmemory.ISpreadLoader;
import jp.co.yahoo.yosegi.spark.inmemory.SparkLoaderFactoryUtil;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SparkMapLoader implements ISpreadLoader<WritableColumnVector> {
  private final WritableColumnVector vector;
  private final int loadSize;
  private final boolean compositeDataType;

  private int totalChildCount;
  private int currentChildCount;
  private WritableColumnVector[] childVector;
  private String[] childKeys;
  private int numNulls;

  public SparkMapLoader(final WritableColumnVector vector, final int loadSize) {
    this.vector = vector;
    this.loadSize = loadSize;
    numNulls = 0;
    compositeDataType = isCompositeDataType(vector.getChild(1).dataType().getClass());
  }

  private boolean isCompositeDataType(final Class klass) {
    if (klass == MapType.class || klass == ArrayType.class || klass == StructType.class) {
      return true;
    }
    return false;
  }

  @Override
  public int getLoadSize() {
    return loadSize;
  }

  @Override
  public void setNull(final int index) throws IOException {
    vector.putNull(index);
  }

  @Override
  public void finish() throws IOException {
    if (compositeDataType) {
      return;
    }
    // NOTE: set key-value pairs without null value
    final int capacity = loadSize * totalChildCount - numNulls;
    vector.getChild(0).reserve(capacity);
    vector.getChild(1).reserve(capacity);
    int offset = 0;
    int count = 0;
    for (int i = 0; i < loadSize; i++) {
      for (int j = 0; j < totalChildCount; j++) {
        if (childVector[j].isNullAt(i)) {
          continue;
        }
        vector.getChild(0).putByteArray(offset, childKeys[j].getBytes(StandardCharsets.UTF_8));
        final Class klass = vector.getChild(1).dataType().getClass();
        if (klass == DataTypes.StringType.getClass() || klass == DataTypes.BinaryType.getClass()) {
          vector.getChild(1).putByteArray(offset, childVector[j].getBinary(i));
        } else if (klass == DataTypes.BooleanType.getClass()) {
          vector.getChild(1).putBoolean(offset, childVector[j].getBoolean(i));
        } else if (klass == DataTypes.LongType.getClass()) {
          vector.getChild(1).putLong(offset, childVector[j].getLong(i));
        } else if (klass == DataTypes.IntegerType.getClass()) {
          vector.getChild(1).putInt(offset, childVector[j].getInt(i));
        } else if (klass == DataTypes.ShortType.getClass()) {
          vector.getChild(1).putShort(offset, childVector[j].getShort(i));
        } else if (klass == DataTypes.ByteType.getClass()) {
          vector.getChild(1).putByte(offset, childVector[j].getByte(i));
        } else if (klass == DataTypes.DoubleType.getClass()) {
          vector.getChild(1).putDouble(offset, childVector[j].getDouble(i));
        } else if (klass == DataTypes.FloatType.getClass()) {
          vector.getChild(1).putFloat(offset, childVector[j].getFloat(i));
        } else if (klass == DecimalType.class) {
          final DecimalType dt = ((DecimalType) vector.getChild(1).dataType());
          vector
              .getChild(1)
              .putDecimal(
                  offset, childVector[j].getDecimal(i, dt.precision(), dt.scale()), dt.precision());
        } else if (klass == DataTypes.TimestampType.getClass()) {
          vector.getChild(1).putLong(offset, childVector[j].getLong(i));
        } else {
          vector.getChild(1).putNull(offset);
        }
        offset++;
        count++;
      }
      vector.putArray(i, offset - count, count);
      count = 0;
    }
  }

  @Override
  public WritableColumnVector build() throws IOException {
    return vector;
  }

  @Override
  public void setChildCount(final int childCount) throws IOException {
    childVector = new OnHeapColumnVector[childCount];
    totalChildCount = childCount;
    currentChildCount = 0;
    childKeys = new String[childCount];
    if (compositeDataType) {
      // FIXME:
      //      for (int i = 0; i < loadSize; i++) {
      //        final int capacity = loadSize * childCount;
      //        vector.getChild(0).reserve(capacity);
      //        vector.getChild(1).reserve(capacity);
      //        vector.putArray(i, i * totalChildCount, totalChildCount);
      //      }
    }
  }

  @Override
  public void loadChild(final ColumnBinary columnBinary, final int loadSize) throws IOException {
    if (compositeDataType) {
      // FIXME:
      //      for (int i = 0; i < loadSize; i++) {
      //        vector
      //            .getChild(0)
      //            .putByteArray(
      //                i * totalChildCount + currentChildCount,
      //                columnBinary.columnName.getBytes(StandardCharsets.UTF_8));
      //      }
      //      SparkLoaderFactoryUtil.createLoaderFactory(vector.getChild(1)).create(columnBinary,
      // loadSize);
    } else {
      childVector[currentChildCount] =
          new OnHeapColumnVector(loadSize, vector.getChild(1).dataType());
      childKeys[currentChildCount] = columnBinary.columnName;
      SparkLoaderFactoryUtil.createLoaderFactory(childVector[currentChildCount])
          .create(columnBinary, loadSize);
      numNulls += childVector[currentChildCount].numNulls();
      currentChildCount++;
    }
  }
}
