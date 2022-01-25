package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkConstShortLoader extends AbstractSparkConstNumberLoader {
  public SparkConstShortLoader(final WritableColumnVector vector, final int loadSize) {
    super(vector, loadSize);
  }

  @Override
  public void setConstFromPrimitiveObject(final PrimitiveObject value) throws IOException {
    if (value == null) {
      setConstFromNull();
    } else {
      try {
        setConstFromShort(value.getShort());
      } catch (final Exception e) {
        setConstFromNull();
      }
    }
  }

  @Override
  public void setConstFromShort(final short value) throws IOException {
    vector.putShorts(0, loadSize, value);
  }
}
