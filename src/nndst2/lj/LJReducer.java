package nndst.lj;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import nndst.serializer.NeighborArrayWritable;
import nndst.serializer.NeighborWritable;
import nndst.serializer.NodeWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

public class LJReducer extends
		Reducer<NodeWritable, NeighborArrayWritable, NodeWritable, NeighborArrayWritable> {
	
	public static enum UpdateCounter {
		CONVERGED
	}
	
	private int K;
	private final int defaultK =20;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration conf =context.getConfiguration();
		
		K =conf.getInt("param.k", defaultK);
	}
	
	@Override
	protected void reduce(NodeWritable key, Iterable<NeighborArrayWritable> invals, Context context)
			throws IOException, InterruptedException {
		int knnCounter =0;
		
		NeighborWritable[]    tmpL;
		NeighborWritable[]    knnL   =new NeighborWritable[K];
		NeighborArrayWritable outval =new NeighborArrayWritable();
		
		Set<NeighborWritable> ljL =new HashSet<NeighborWritable>();
		
//		System.out.println(key.getIntNode() + "->");
		for (NeighborArrayWritable inval : invals) {
//			System.out.println(inval);
			tmpL =(NeighborWritable[])inval.toArray();
			
			for (int i =0; i < tmpL.length; i++) {
				if (tmpL[i].getBooleanSymbol()) {
					knnL[knnCounter++] =tmpL[i].clone();
//					knnL[knnCounter].markNewAsNeutral();
//					knnL[knnCounter++].markNewRAsNeutral();
				} else {
					ljL.add(tmpL[i].clone());
				}
			}
		}
		
		Arrays.sort(knnL);
		for (NeighborWritable nw : ljL) {
			if (isNeighbor(nw.getIntNode(), knnL)) continue;
			
			if (nw.getFloatDist() < knnL[K - 1].getFloatDist()) {
				knnL[K - 1] =nw.clone();
				knnL[K - 1].markSymbolAsTrue();
				
				Arrays.sort(knnL);
				context.getCounter(UpdateCounter.CONVERGED).increment(1);
			}
		}
		
		outval.set(knnL);
		context.write(key, outval);
	}
	
	boolean isNeighbor(int node, NeighborWritable[] list) {
		boolean flag =false;
		
		for (int i =0; i < list.length; i++) {
			if (list[i].getIntNode() == node)
				{ flag =true; break; }
		}
		
		return flag;
	}
}
