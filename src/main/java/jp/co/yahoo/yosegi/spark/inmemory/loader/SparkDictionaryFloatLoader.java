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
    public int decodeToInt(int id) {
      return (int) dic[id];
    }

    @Override
    public long decodeToLong(int id) {
      return (long) dic[id];
    }

    @Override
    public float decodeToFloat(int id) {
      return dic[id];
    }

    @Override
    public double decodeToDouble(int id) {
      return dic[id];
    }

    public void set(final int index, final float value) {
      dic[index] = value;
    }
  }

  public SparkDictionaryFloatLoader(WritableColumnVector vector, int loadSize) {
    super(vector, loadSize);
  }

  @Override
  public void setPrimitiveObjectToDic(int index, PrimitiveObject value) throws IOException {
    if (value == null) {
      setNullToDic(index);
    } else {
      try {
        setFloatToDic(index, value.getFloat());
      } catch (Exception e) {
        setNullToDic(index);
      }
    }
  }

  @Override
  public void createDictionary(int dictionarySize) throws IOException {
    isNull = new boolean[dictionarySize];
    idxVector = vector.reserveDictionaryIds(dictionarySize);
    dic = new SparkFloatDictionary(dictionarySize);
    vector.setDictionary(dic);
  }

  @Override
  public void setFloatToDic(int index, float value) throws IOException {
    ((SparkFloatDictionary) dic).set(index, value);
  }
}
