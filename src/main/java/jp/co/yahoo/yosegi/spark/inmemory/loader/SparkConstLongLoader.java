package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkConstLongLoader extends AbstractSparkConstNumberLoader {
  private class SparkLongDictionary implements ISparkDictionary {
    private final long[] dic;

    public SparkLongDictionary(final int dicSize) {
      this.dic = new long[dicSize];
    }

    @Override
    public int decodeToInt(final int id) {
      return (int) dic[id];
    }

    @Override
    public long decodeToLong(final int id) {
      return dic[id];
    }

    @Override
    public float decodeToFloat(final int id) {
      return dic[id];
    }

    @Override
    public double decodeToDouble(final int id) {
      return dic[id];
    }

    public void set(final int index, final long value) {
      dic[index] = value;
    }
  }

  public SparkConstLongLoader(final WritableColumnVector vector, final int loadSize) {
    super(vector, loadSize);
    idxVector = this.vector.reserveDictionaryIds(loadSize);
    dic = new SparkLongDictionary(1);
    this.vector.setDictionary(dic);
  }

  @Override
  public void setConstFromPrimitiveObject(final PrimitiveObject value) throws IOException {
    if (value == null) {
      setConstFromNull();
    } else {
      try {
        setConstFromLong(value.getLong());
      } catch (final Exception e) {
        setConstFromNull();
      }
    }
  }

  @Override
  public void setConstFromLong(final long value) throws IOException {
    ((SparkLongDictionary) dic).set(0, value);
    idxVector.putInts(0, loadSize, 0);
  }
}
