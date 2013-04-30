package nndst.serializer;

import org.apache.hadoop.io.ArrayWritable;

public class NeighborArrayWritable extends ArrayWritable {
	public NeighborArrayWritable() {
		super(NeighborWritable.class);
	}
}
