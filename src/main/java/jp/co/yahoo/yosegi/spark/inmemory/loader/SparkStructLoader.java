package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.inmemory.ILoaderFactory;
import jp.co.yahoo.yosegi.inmemory.ISpreadLoader;
import jp.co.yahoo.yosegi.spark.inmemory.SparkLoaderFactoryUtil;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SparkStructLoader implements ISpreadLoader<WritableColumnVector> {
  private final WritableColumnVector vector;
  private final int loadSize;
  private final Map<String, ILoaderFactory> loaderFactoryMap = new HashMap<>();

  public SparkStructLoader(final WritableColumnVector vector, final int loadSize) {
    this.vector = vector;
    this.loadSize = loadSize;
    StructType structType = (StructType) vector.dataType();
    String[] names = structType.fieldNames();
    for (int i = 0; i < names.length; i++) {
      vector.getChild(i).reserve(loadSize);
      loaderFactoryMap.put(names[i], SparkLoaderFactoryUtil.createLoaderFactory(vector.getChild(i)));
    }
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
    // FIXME:
  }

  @Override
  public WritableColumnVector build() throws IOException {
    return vector;
  }

  @Override
  public void loadChild(ColumnBinary columnBinary, int loadSize) throws IOException {
    if (loaderFactoryMap.containsKey(columnBinary.columnName)) {
      loaderFactoryMap.get(columnBinary.columnName).create(columnBinary, loadSize);
    } else {
      // FIXME:
    }
  }
}
