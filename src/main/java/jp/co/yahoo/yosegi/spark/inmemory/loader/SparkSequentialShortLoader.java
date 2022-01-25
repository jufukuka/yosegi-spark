package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkSequentialShortLoader extends AbstractSparkSequentialNumberLoader {
  public SparkSequentialShortLoader(WritableColumnVector vector, int loadSize) {
    super(vector, loadSize);
  }

  @Override
  public void setPrimitiveObject(int index, PrimitiveObject value) throws IOException {
    if (value == null) {
      setNull(index);
    } else {
      try {
        setShort(index, value.getShort());
      } catch (Exception e) {
        setNull(index);
      }
    }
  }

  @Override
  public void setShort(int index, short value) throws IOException {
    vector.putShort(index, value);
  }
}
