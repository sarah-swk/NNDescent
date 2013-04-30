package nndst.serializer;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;

public class FloatArrayWritable extends ArrayWritable {
	public FloatArrayWritable() {
		super(FloatWritable.class);
	}
}