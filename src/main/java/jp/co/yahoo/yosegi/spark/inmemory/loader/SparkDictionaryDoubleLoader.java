package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkDictionaryDoubleLoader extends AbstractSparkDictionaryNumberLoader {
  private class SparkDoubleDictionary implements ISparkDictionary {
    private final double[] dic;

    public SparkDoubleDictionary(final int dicSize) {
      this.dic = new double[dicSize];
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
      return (float) dic[id];
    }

    @Override
    public double decodeToDouble(final int id) {
      return dic[id];
    }

    public void set(final int index, final double value) {
      dic[index] = value;
    }
  }

  public SparkDictionaryDoubleLoader(final WritableColumnVector vector, final int loadSize) {
    super(vector, loadSize);
  }

  @Override
  public void setPrimitiveObjectToDic(final int index, final PrimitiveObject value) throws IOException {
    if (value == null) {
      setNullToDic(index);
    } else {
      try {
        setDoubleToDic(index, value.getDouble());
      } catch (final Exception e) {
        setNullToDic(index);
      }
    }
  }

  @Override
  public void createDictionary(final int dictionarySize) throws IOException {
    isNull = new boolean[dictionarySize];
    idxVector = vector.reserveDictionaryIds(dictionarySize);
    dic = new SparkDoubleDictionary(dictionarySize);
    vector.setDictionary(dic);
  }

  @Override
  public void setDoubleToDic(final int index, final double value) throws IOException {
    ((SparkDoubleDictionary) dic).set(index, value);
  }
}
