package jp.co.yahoo.yosegi.spark.inmemory.loader;

import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkConstNullArrayLoader extends AbstractSparkConstLoader {
    public SparkConstNullArrayLoader(final WritableColumnVector vector, final int loadSize) {
        super(vector, loadSize);
    }

    @Override
    public void setNull( final int index ) throws IOException {
        // FIXME
    }

    @Override
    public void setConstFromNull() throws IOException {
        // FIXME
        for (int i = 0; i < loadSize; i++) {
            vector.putArray(i, 0, 0);
        }
    }

    @Override
    public void setConstFromBytes( final byte[] value ) throws IOException {
        // FIXME
        setConstFromNull();
    }

    @Override
    public void setConstFromBytes(
        final byte[] value , final int start , final int length ) throws IOException {
        // FIXME
        setConstFromNull();
    }

    @Override
    public void setConstFromString( final char[] value ) throws IOException {
        // FIXME
        setConstFromNull();
    }

    @Override
    public void setConstFromString(
        final char[] value ,
        final int start ,
        final int length ) throws IOException {
        // FIXME
        setConstFromNull();
    }
}
