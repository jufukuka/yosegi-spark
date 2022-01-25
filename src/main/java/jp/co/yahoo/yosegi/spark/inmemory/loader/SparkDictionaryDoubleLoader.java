package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkDictionaryDoubleLoader extends AbstractSparkDictionaryNumberLoader{
  private class SparkDoubleDictionary implements ISparkDictionary {
    private final double[] dic;

    public SparkDoubleDictionary(final int dicSize) {
      this.dic = new double[dicSize];
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
      return (float) dic[id];
    }

    @Override
    public double decodeToDouble(int id) {
      return dic[id];
    }

    public void set(final int index, final double value) {
      dic[index] = value;
    }
  }

  public SparkDictionaryDoubleLoader(WritableColumnVector vector, int loadSize) {
    super(vector, loadSize);
  }

  @Override
  public void setPrimitiveObjectToDic(int index, PrimitiveObject value) throws IOException {
    if (value == null) {
      setNullToDic(index);
    } else {
      try {
        setDoubleToDic(index, value.getDouble());
      } catch (Exception e) {
        setNullToDic(index);
      }
    }
  }

  @Override
  public void createDictionary(int dictionarySize) throws IOException {
    isNull = new boolean[dictionarySize];
    idxVector = vector.reserveDictionaryIds(dictionarySize);
    dic = new SparkDoubleDictionary(dictionarySize);
    vector.setDictionary(dic);
  }

  @Override
  public void setDoubleToDic(int index, double value) throws IOException {
    ((SparkDoubleDictionary) dic).set(index, value);
  }
}
