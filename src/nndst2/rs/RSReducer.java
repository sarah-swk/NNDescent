package nndst.rs;

import java.io.IOException;
import java.util.ArrayList;

import nndst.serializer.NeighborArrayWritable;
import nndst.serializer.NeighborWritable;
import nndst.serializer.NodeWritable;

import org.apache.hadoop.mapreduce.Reducer;

public class RSReducer extends 
	Reducer<NodeWritable, NeighborWritable, NodeWritable, NeighborArrayWritable> 
{
	@Override
	protected void reduce(NodeWritable inkey, Iterable<NeighborWritable> invals, Context context)
		throws IOException, InterruptedException 
	{
		ArrayList<NeighborWritable> revl   =new ArrayList<NeighborWritable>();
		NeighborArrayWritable       outval =new NeighborArrayWritable();
		
		for (NeighborWritable inval : invals)
			revl.add(inval.clone());
		
		outval.set(revl.toArray(new NeighborWritable[revl.size()]));
		
		context.write(inkey, outval);
	}
}
