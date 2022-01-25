package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkSequentialDoubleLoader extends AbstractSparkSequentialNumberLoader {
  public SparkSequentialDoubleLoader(WritableColumnVector vector, int loadSize) {
    super(vector, loadSize);
  }

  @Override
  public void setPrimitiveObject(int index, PrimitiveObject value) throws IOException {
    if (value == null) {
      setNull(index);
    } else {
      try {
        setDouble(index, value.getDouble());
      } catch (Exception e) {
        setNull(index);
      }
    }
  }

  @Override
  public void setDouble(int index, double value) throws IOException {
    vector.putDouble(index, value);
  }
}
