package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkConstByteLoader extends AbstractSparkConstNumberLoader {
  public SparkConstByteLoader(WritableColumnVector vector, int loadSize) {
    super(vector, loadSize);
  }

  @Override
  public void setConstFromPrimitiveObject(PrimitiveObject value) throws IOException {
    if (value == null) {
      setConstFromNull();
    } else {
      try {
        setConstFromByte(value.getByte());
      } catch (Exception e) {
        setConstFromNull();
      }
    }
  }

  @Override
  public void setConstFromByte(byte value) throws IOException {
    vector.putBytes(0, loadSize, value);
  }
}
