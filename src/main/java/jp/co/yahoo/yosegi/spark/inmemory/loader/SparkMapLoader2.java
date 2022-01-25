package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.inmemory.ISpreadLoader;
import jp.co.yahoo.yosegi.spark.inmemory.SparkLoaderFactoryUtil;
import jp.co.yahoo.yosegi.spark.inmemory.SparkMapChildLoaderFactoryUtil;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SparkMapLoader2 implements ISpreadLoader<WritableColumnVector> {
  private final WritableColumnVector vector;
  private final int loadSize;
  private final boolean compositeDataType;

  private int totalChildCount;
  private int currentChildCount;
  private WritableColumnVector[] childVector;
  private String[] childKeys;
  private int numNulls;

  public SparkMapLoader2(WritableColumnVector vector, int loadSize) {
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
  public void setNull(int index) throws IOException {
    vector.putNull(index);
  }

  @Override
  public void finish() throws IOException {
    System.out.println(vector.getChild(1).dataType().toString());
    Class valueClass = vector.getChild(1).dataType().getClass();
    if (compositeDataType) {
      return;
    }
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
        Class klass = vector.getChild(1).dataType().getClass();
        if (klass == DataTypes.FloatType.getClass()) {
          vector.getChild(1).putFloat(offset, childVector[j].getFloat(i));
        } else if (klass == DataTypes.StringType.getClass() || klass == DataTypes.BinaryType.getClass()) {
          vector.getChild(1).putByteArray(offset, childVector[j].getBinary(i));
        } else if (isCompositeDataType(klass)) {
          // FIXME: ありえない
          vector.getChild(1).putNull(offset);
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
  public void setChildCount(int childCount) throws IOException {
    childVector = new OnHeapColumnVector[childCount];
    totalChildCount = childCount;
    currentChildCount = 0;
    childKeys = new String[childCount];
    if (compositeDataType) {
      for (int i = 0; i < loadSize; i++) {
        final int capacity = loadSize * childCount;
        vector.getChild(0).reserve(capacity);
        vector.getChild(1).reserve(capacity);
        vector.putArray(i, i * totalChildCount, totalChildCount);
      }
    }
  }

  @Override
  public void loadChild(ColumnBinary columnBinary, int loadSize) throws IOException {
    if (compositeDataType) {
      for (int i = 0; i < loadSize; i++) {
        vector.getChild(0).putByteArray(i * totalChildCount + currentChildCount, columnBinary.columnName.getBytes(StandardCharsets.UTF_8));
      }
      SparkLoaderFactoryUtil.createLoaderFactory(vector.getChild(1)).create(columnBinary, loadSize);
    } else {
      childVector[currentChildCount] = new OnHeapColumnVector(loadSize, vector.getChild(1).dataType());
      childKeys[currentChildCount] = columnBinary.columnName;
      SparkLoaderFactoryUtil.createLoaderFactory(childVector[currentChildCount]).create(columnBinary, loadSize);
      numNulls += childVector[currentChildCount].numNulls();
      currentChildCount++;
    }
  }
}
