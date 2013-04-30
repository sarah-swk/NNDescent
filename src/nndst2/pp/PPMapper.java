package nndst.pp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import nndst.serializer.NeighborArrayWritable;
import nndst.serializer.NeighborWritable;
import nndst.serializer.NodeWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class PPMapper extends 
		Mapper<NodeWritable, NeighborArrayWritable, NodeWritable, NeighborArrayWritable> {
	private int   K;
	private float RHO;
	
	private final int defaultK     =20;
	private final float defaultRHO =(float) 0.5;
	
	private int SampleSize;
	
//	private Map<NodeWritable, NeighborWritable[]> knnLists;
//	private List<PairWritable> revSets;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration conf =context.getConfiguration();
		
		K   =conf.getInt("param.k", defaultK);
		RHO =conf.getFloat("param.rho", defaultRHO);
		
		SampleSize =(int) (RHO*K);
		
//		knnLists    =new HashMap<NodeWritable, NeighborWritable[]>();
//		revSets =new ArrayList<PairWritable>();
	}

	@Override
	protected void map(NodeWritable inkey, NeighborArrayWritable inval, Context context) 
			throws IOException, InterruptedException 
	{
//		System.out.println(inkey + "\t" + inval);
		NeighborWritable[]    knnList =(NeighborWritable[])inval.toArray();
		NeighborWritable[]    tmpList =new NeighborWritable[1];
		NeighborArrayWritable outval  =new NeighborArrayWritable();
		
		NeighborWritable[] Old =getOld(knnList);
		NeighborWritable[] New =getNew(knnList);
		
		Map<NodeWritable, NeighborWritable> OldR =getOldR(inkey, Old);
		Map<NodeWritable, NeighborWritable> NewR =getNewR(inkey, New);
		
//		for (int i =0; i < K; i++) context.write(inkey, knnList[i]);
		outval.set(knnList);
		context.write(inkey, outval);
		
		for (Entry<NodeWritable, NeighborWritable> e : OldR.entrySet()) {
//			revSets.add(new PairWritable(e.getKey(), e.getValue()));
			tmpList[0] =e.getValue(); outval.set(tmpList);
			context.write(e.getKey(), outval);
		}
		
		for (Entry<NodeWritable, NeighborWritable> e : NewR.entrySet()) { 
//			revSets.add(new PairWritable(e.getKey(), e.getValue()));
			tmpList[0] =e.getValue(); outval.set(tmpList);
			context.write(e.getKey(), outval);
		}
	}
	
//	@Override
//	protected void cleanup(Context context) throws IOException, InterruptedException {
//		super.cleanup(context);
//		
//		Map<NodeWritable, List<NeighborWritable> > revList 
//				=new HashMap<NodeWritable, List<NeighborWritable> >();
//		NeighborArrayWritable outval;
//		
//		for (PairWritable p : revSets) {
//			if (revList.containsKey(p.getKey())) {
//				List<NeighborWritable> tmpL =revList.get(p.getKey());
//				
//				tmpL.add(p.getValue().clone());
//			} else {
//				List<NeighborWritable> tmpL =new ArrayList<NeighborWritable>();
//				tmpL.add(p.getValue());
//				revList.put(p.getKey().clone(), tmpL);
//			}
//		}
//		
//		for (Map.Entry<NodeWritable, List<NeighborWritable> > e : revList.entrySet()) {
//			outval =new NeighborArrayWritable();
//			outval.set(e.getValue().toArray(new NeighborWritable[e.getValue().size()]));
//			context.write(e.getKey(), outval);
//		}
//			
//	}
	
	protected NeighborWritable[] getOld(NeighborWritable[] knnList) 
	{
		int count =0;
		
		ArrayList<NeighborWritable> Old =new ArrayList<NeighborWritable>();
		
		for (int i =0; i < knnList.length; i++) {
//			if (knnList[i].isFlagFalse()) {
			if (knnList[i].getIntRemain() >= 1) {
//				knnList[i].markNewAsFalse();
				knnList[i].incrementRemain();
				Old.add(count++, knnList[i]);
			}
		}
		
		return Old.toArray(new NeighborWritable[Old.size()]);
	}
	
	protected NeighborWritable[] getNew(NeighborWritable[] knnList) 
	{
		int r, counter =0;
		int[] newIndex =new int[K];
		
		Random rnd =new Random();
		
		NeighborWritable[] New;
//		NeighborWritable[] tmpNew =new NeighborWritable[K];
		
		for (int i =0; i < knnList.length; i++) 
			if (knnList[i].getIntRemain() == 0) newIndex[counter++] =i;
		
		if (counter > SampleSize) {
			for (int i =0; i < SampleSize; i++) {
				r = rnd.nextInt(counter);
				
				if (knnList[newIndex[r]].getIntRemain() == 1) { i--; continue; }
//				knnList[newIndex[r]].markNewAsTrue();
//				knnList[newIndex[r]].markFlagAsFalse();
				knnList[newIndex[r]].incrementRemain();
			}
		} else {
			for (int i =0; i < counter; i++) knnList[newIndex[i]].incrementRemain();
		}
		
		New =new NeighborWritable[counter];
		for (int i =0; i < counter; i++) New[i] =knnList[newIndex[i]];
		
		return New;
	}
	
	protected Map<NodeWritable, NeighborWritable> 
			getOldR(NodeWritable inkey, NeighborWritable[] Old) 
	{
		Map<NodeWritable, NeighborWritable> rev =new HashMap<NodeWritable, NeighborWritable>();
		
		NodeWritable tmpNode;
		NeighborWritable tmpItem;
		
		for (int i =0; i < Old.length; i++) {
			tmpNode =new NodeWritable(Old[i].getWritableNode(), Old[i].getWritablePoint());
			tmpItem =new NeighborWritable(inkey.getWritableNode(), inkey.getWritablePoint(), new FloatWritable(0));
			
			tmpItem.setRemain(Old[i].getIntRemain());
			tmpItem.markSymbolAsFalse();
			
			rev.put(tmpNode, tmpItem);
		}
		
		return rev;
	}
	
	protected Map<NodeWritable, NeighborWritable>
			getNewR(NodeWritable inkey, NeighborWritable[] New) 
	{
		Map<NodeWritable, NeighborWritable> rev =new HashMap<NodeWritable, NeighborWritable>();
		
		NodeWritable tmpNode;
		NeighborWritable tmpItem;
		
		for (int i =0; i < New.length; i++) {
			if (New[i].getIntRemain() == 1) {
				tmpNode =new NodeWritable(New[i].getWritableNode(), New[i].getWritablePoint());
				tmpItem =new NeighborWritable(inkey.getWritableNode(), inkey.getWritablePoint(), new FloatWritable(0));

				tmpItem.setRemain(1);
				tmpItem.markSymbolAsFalse();

				rev.put(tmpNode, tmpItem);
			}
		}
		
		return rev;
	}
	
	//TODO: Increment "remain"
}
