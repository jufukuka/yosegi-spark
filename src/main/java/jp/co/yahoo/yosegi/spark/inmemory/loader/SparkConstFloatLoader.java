package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkConstFloatLoader extends AbstractSparkConstNumberLoader {
  private class SparkFloatDictionary implements ISparkDictionary {
    private final float[] dic;

    public SparkFloatDictionary(final int dicSize) {
      this.dic = new float[dicSize];
    }

    @Override
    public int decodeToInt(final int id) {
      return (int) dic[id];
    }

    @Override
    public long decodeToLong(final int id) {
      return (long) dic[id];
    }

    @Override
    public float decodeToFloat(final int id) {
      return dic[id];
    }

    @Override
    public double decodeToDouble(final int id) {
      return dic[id];
    }

    public void set(final int index, final float value) {
      dic[index] = value;
    }
  }

  public SparkConstFloatLoader(final WritableColumnVector vector, final int loadSize) {
    super(vector, loadSize);
    idxVector = this.vector.reserveDictionaryIds(loadSize);
    dic = new SparkFloatDictionary(1);
    this.vector.setDictionary(dic);
  }

  @Override
  public void setConstFromPrimitiveObject(final PrimitiveObject value) throws IOException {
    if (value == null) {
      setConstFromNull();
    } else {
      try {
        setConstFromFloat(value.getFloat());
      } catch (final Exception e) {
        setConstFromNull();
      }
    }
  }

  @Override
  public void setConstFromFloat(final float value) throws IOException {
    ((SparkFloatDictionary) dic).set(0, value);
    idxVector.putInts(0, loadSize, 0);
  }
}
