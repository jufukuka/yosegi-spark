package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkDictionaryLongLoader extends AbstractSparkDictionaryNumberLoader {
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

  public SparkDictionaryLongLoader(final WritableColumnVector vector, final int loadSize) {
    super(vector, loadSize);
  }

  @Override
  public void setPrimitiveObjectToDic(final int index, final PrimitiveObject value) throws IOException {
    if (value == null) {
      setNullToDic(index);
    } else {
      try {
        setLongToDic(index, value.getLong());
      } catch (final Exception e) {
        setNullToDic(index);
      }
    }
  }

  @Override
  public void createDictionary(final int dictionarySize) throws IOException {
    isNull = new boolean[dictionarySize];
    idxVector = vector.reserveDictionaryIds(loadSize);
    dic = new SparkLongDictionary(dictionarySize);
    vector.setDictionary(dic);
  }

  @Override
  public void setLongToDic(final int index, final long value) throws IOException {
    ((SparkLongDictionary) dic).set(index, value);
  }
}
