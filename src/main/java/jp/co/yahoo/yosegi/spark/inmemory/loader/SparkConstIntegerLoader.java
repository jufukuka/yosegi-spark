package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkConstIntegerLoader extends AbstractSparkConstNumberLoader {
  private class SparkIntegerDictionary implements ISparkDictionary {
    private final int[] dic;

    public SparkIntegerDictionary(final int dicSize) {
      this.dic = new int[dicSize];
    }

    @Override
    public int decodeToInt(final int id) {
      return dic[id];
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

    public void set(final int index, final int value) {
      dic[index] = value;
    }
  }

  public SparkConstIntegerLoader(final WritableColumnVector vector, final int loadSize) {
    super(vector, loadSize);
    idxVector = this.vector.reserveDictionaryIds(loadSize);
    dic = new SparkIntegerDictionary(1);
    this.vector.setDictionary(dic);
  }

  @Override
  public void setConstFromPrimitiveObject(final PrimitiveObject value) throws IOException {
    if (value == null) {
      setConstFromNull();
    } else {
      try {
        setConstFromInteger(value.getInt());
      } catch (final Exception e) {
        setConstFromNull();
      }
    }
  }

  @Override
  public void setConstFromInteger(final int value) throws IOException {
    ((SparkIntegerDictionary) dic).set(0, value);
    idxVector.putInts(0, loadSize, 0);
  }
}
