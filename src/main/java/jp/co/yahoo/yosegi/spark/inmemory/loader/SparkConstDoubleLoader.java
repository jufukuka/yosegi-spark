package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkConstDoubleLoader extends AbstractSparkConstNumberLoader {
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

  public SparkConstDoubleLoader(WritableColumnVector vector, int loadSize) {
    super(vector, loadSize);
    idxVector = this.vector.reserveDictionaryIds(loadSize);
    dic = new SparkDoubleDictionary(1);
    this.vector.setDictionary(dic);
  }

  @Override
  public void setConstFromPrimitiveObject(PrimitiveObject value) throws IOException {
    if (value == null) {
      setConstFromNull();
    } else {
      try {
        setConstFromDouble(value.getDouble());
      } catch (Exception e) {
        setConstFromNull();
      }
    }
  }

  @Override
  public void setConstFromDouble(double value) throws IOException {
    ((SparkDoubleDictionary) dic).set(0, value);
    idxVector.putInts(0, loadSize, 0);
  }
}
