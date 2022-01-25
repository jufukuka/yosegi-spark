package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkDictionaryFloatLoader extends AbstractSparkDictionaryNumberLoader {
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

  public SparkDictionaryFloatLoader(final WritableColumnVector vector, final int loadSize) {
    super(vector, loadSize);
  }

  @Override
  public void setPrimitiveObjectToDic(final int index, final PrimitiveObject value) throws IOException {
    if (value == null) {
      setNullToDic(index);
    } else {
      try {
        setFloatToDic(index, value.getFloat());
      } catch (final Exception e) {
        setNullToDic(index);
      }
    }
  }

  @Override
  public void createDictionary(final int dictionarySize) throws IOException {
    isNull = new boolean[dictionarySize];
    idxVector = vector.reserveDictionaryIds(dictionarySize);
    dic = new SparkFloatDictionary(dictionarySize);
    vector.setDictionary(dic);
  }

  @Override
  public void setFloatToDic(final int index, final float value) throws IOException {
    ((SparkFloatDictionary) dic).set(index, value);
  }
}
