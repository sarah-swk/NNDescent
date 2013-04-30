package nndst.rs;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import nndst.serializer.NeighborArrayWritable;
import nndst.serializer.NeighborWritable;
import nndst.serializer.NodeWritable;

import org.apache.hadoop.mapreduce.Reducer;

public class RSAVReducer extends
	Reducer<NodeWritable, NeighborArrayWritable, NodeWritable, NeighborArrayWritable> 
{
	@Override
	protected void reduce(NodeWritable inkey, Iterable<NeighborArrayWritable> invals, Context context)
		throws IOException, InterruptedException
	{
		Set<NeighborWritable> knnl;
		Set<NeighborWritable> revl; 
		NeighborWritable[]    invAry;
		NeighborArrayWritable outval;
		
		knnl =new HashSet<NeighborWritable>();
		revl =new HashSet<NeighborWritable>();
		outval =new NeighborArrayWritable();
		
		for (NeighborArrayWritable inval : invals) {
			invAry =(NeighborWritable[]) inval.toArray();
			
			for (int i =0; i < invAry.length; i++) {
//				if (!invAry[i].isFlagNeutral())
				if (invAry[i].getBooleanSymbol())
					knnl.add(invAry[i].clone());
				else
					revl.add(invAry[i].clone());
			}
		}
		
		for (NeighborWritable rev : revl) {
			if (knnl.contains(revl)) continue;
			else                     knnl.add(rev);
		}
		
		outval.set(knnl.toArray(new NeighborWritable[knnl.size()]));
		
		context.write(inkey, outval);
	}
}
