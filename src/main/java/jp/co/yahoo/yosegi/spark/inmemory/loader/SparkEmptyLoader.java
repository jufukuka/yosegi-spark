package jp.co.yahoo.yosegi.spark.inmemory.loader;

import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;

public class SparkEmptyLoader {
    public static void load(final WritableColumnVector vector, final int loadSize) throws IOException {
        System.out.println("SparkEmptyLoader.load: " + loadSize);
        final Class klass = vector.dataType().getClass();
        if (klass == ArrayType.class) {
            new SparkEmptyArrayLoader(vector, loadSize).build();
        } else if (klass == StructType.class) {
            new SparkEmptyStructLoader(vector, loadSize).build();
        } else if (klass == MapType.class) {
            new SparkEmptyMapLoader(vector, loadSize).build();
        } else {
            new SparkNullLoader(vector, loadSize).build();
        }
    }
}
