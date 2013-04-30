package nndst.pp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import nndst.serializer.NeighborArrayWritable;
import nndst.serializer.NeighborWritable;
import nndst.serializer.NodeWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

public class PPReducer 
		extends Reducer<NodeWritable, NeighborArrayWritable, NodeWritable, NeighborArrayWritable> {
	private int   K;
	private float RHO;
	
	private final int defaultK =20;
	private final float defaultRHO =(float) 0.5;
	
	private int SampleSize;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration conf =context.getConfiguration();
		
		K   =conf.getInt("param.k", defaultK);
		RHO =conf.getFloat("param.rho", defaultRHO);
		
		SampleSize =(int) (RHO*K);
	}
	
	@Override
	protected void reduce(NodeWritable inkey, Iterable<NeighborArrayWritable> invals, Context context) 
			throws IOException, InterruptedException 
	{
		int counter =0;
		
		Map<Integer, NeighborWritable> Old  =new HashMap<Integer, NeighborWritable>();
		Map<Integer, NeighborWritable> New  =new HashMap<Integer, NeighborWritable>();
		Map<Integer, NeighborWritable> OldR =new HashMap<Integer, NeighborWritable>();
		Map<Integer, NeighborWritable> NewR =new HashMap<Integer, NeighborWritable>();
		
		ArrayList<NeighborWritable> knnL =new ArrayList<NeighborWritable>();
		ArrayList<NeighborWritable> tmpL =new ArrayList<NeighborWritable>();
		
		NeighborWritable[]    tmpAry;
		NeighborWritable[]    invAry;
		NeighborArrayWritable outval =new NeighborArrayWritable();
		
//		System.out.println(inkey.getIntNode() + "->");
		for (NeighborArrayWritable inval : invals) {
//			System.out.println(inval);
			invAry =(NeighborWritable[]) inval.toArray();

			for (int i =0; i < invAry.length; i++) {
				if (invAry[i].getBooleanSymbol()) {
					if (invAry[i].getIntRemain() == 1)     New.put(invAry[i].getIntNode(), invAry[i].clone());
					else if (invAry[i].getIntRemain() > 1) Old.put(invAry[i].getIntNode(), invAry[i].clone());
					else                                   knnL.add(invAry[i].clone());
				} else {
					if (invAry[i].getIntRemain() == 1 && !NewR.containsKey(invAry[i].getIntNode()))
						NewR.put(invAry[i].getIntNode(), invAry[i].clone());
					else if (invAry[i].getIntRemain() > 1 && !OldR.containsKey(invAry[i].getIntNode()))
						OldR.put(invAry[i].getIntNode(), invAry[i].clone());
				}
			}
		}
		
		//Union Old
		tmpAry =getSampleList(OldR.values().toArray(new NeighborWritable[OldR.size()]));
		for (int i =0; i < tmpAry.length; i++) {
			if (!Old.containsKey(tmpAry[i].getIntNode()))
				Old.put(tmpAry[i].getIntNode(), tmpAry[i]);
		}
		
		//Union New
		tmpAry =getSampleList(NewR.values().toArray(new NeighborWritable[NewR.size()]));
		for (int i =0; i < tmpAry.length; i++) {
			if (!New.containsKey(tmpAry[i].getIntNode()))
				New.put(tmpAry[i].getIntNode(), tmpAry[i]);
		}
		
		//Output Values
		//TODO: New to Old ni chofuku ga atta baai, Old gawa no yoso wo sakujo
//		System.out.println("New");
		for (Map.Entry<Integer, NeighborWritable> e : New.entrySet()) {
//			System.out.println(e.getValue());
			tmpL.add(counter++, e.getValue());
		}
		
//		System.out.println("Old");
		for (Map.Entry<Integer, NeighborWritable> e : Old.entrySet()) {
//			System.out.println(e.getValue());
			if (!tmpL.contains(e.getValue()) || e.getValue().getBooleanSymbol())
				tmpL.add(counter++, e.getValue());
		}
		
//		System.out.println("knn");
		for (NeighborWritable n : knnL) {
//			System.out.println(n);
			tmpL.add(counter++, n);
		}
		
		tmpAry =tmpL.toArray(new NeighborWritable[tmpL.size()]);
		
		outval.set(tmpAry);
		context.write(inkey, outval);
	}

	private NeighborWritable[] 
			getSampleList(NeighborWritable[] sourceList) 
	{
		int r, count =0;
		
		Random rnd =new Random();
		
		NeighborWritable[] sampleList;
		Map<Integer, NeighborWritable> sampleMap =new HashMap<Integer, NeighborWritable>();
		
		if (sourceList.length > SampleSize) {
			for (int i =0; i < SampleSize; i++) {
				r =rnd.nextInt(sourceList.length);
				
				if (sampleMap.containsKey(sourceList[r].getIntNode()))
					{ i--; continue; }
				else
					sampleMap.put(sourceList[r].getIntNode(), sourceList[r]);
			}
		} else {
			for (int i =0; i < sourceList.length; i++) {
				sampleMap.put(sourceList[i].getIntNode(), sourceList[i]);
			}
		}
		
		sampleList =new NeighborWritable[sampleMap.size()];
		for (Map.Entry<Integer, NeighborWritable> e : sampleMap.entrySet()) {
			sampleList[count++] =e.getValue();
		}
		
		return sampleList.clone();
	}
	
}
