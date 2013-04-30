package nndst.lj;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import nndst.serializer.NeighborArrayWritable;
import nndst.serializer.NeighborWritable;
import nndst.serializer.NodeWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;

public class LJMapper extends 
		Mapper<NodeWritable, NeighborArrayWritable, NodeWritable, NeighborArrayWritable> {
	
	public static enum UpdateCounter {
		CALLDIST
	}
	
	private int K;
	private int defaultK =20;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration conf =context.getConfiguration();
		
		K =conf.getInt("param.k", defaultK);
	}
	
	@Override
	protected void map(NodeWritable key, NeighborArrayWritable inval, Context context)
			throws IOException, InterruptedException 
	{
		int knnCounter;
		
		float dissimNN[][];
		float dissimNO[][];
		
		NodeWritable     tmpNode;
		NeighborWritable tmpNei;
		
		NeighborWritable[] allNnList;
		NeighborWritable[] tmpKnn;
		NeighborWritable[] tmpNew;
		
		NeighborArrayWritable knnAry;
		NeighborArrayWritable newAry;
		NeighborArrayWritable oldAry;
		
		List<NeighborWritable> New;
		List<NeighborWritable> Old;
		
		ArrayBlockingQueue<NeighborWritable> newQue; 
		
		
		knnCounter =0;
		
		knnAry =new NeighborArrayWritable();
		newAry =new NeighborArrayWritable();
		oldAry =new NeighborArrayWritable();
		
		allNnList =(NeighborWritable[]) inval.toArray();
		tmpKnn    =new NeighborWritable[K];
		
		New =new ArrayList<NeighborWritable>();
		Old =new ArrayList<NeighborWritable>();
		
//		System.out.println(key.getIntNode() + "->");
		for (int i =0; i < allNnList.length; i++) {
//			System.out.println(allNnList[i]);
			if (allNnList[i].getBooleanSymbol()) tmpKnn[knnCounter++] =allNnList[i];
			
//			if (allNnList[i].isNewTrue() || allNnList[i].isNewRTrue())
			if (allNnList[i].getIntRemain() == 1)
				New.add(allNnList[i]);
			else if (allNnList[i].getIntRemain() > 1)
				Old.add(allNnList[i]);
		}
		
		knnAry.set(tmpKnn); context.write(key, knnAry);
		
		dissimNN =new float[New.size()][New.size()];
		dissimNO =new float[New.size()][Old.size()];
		
		if (New.size() > 0)
			newQue =new ArrayBlockingQueue<NeighborWritable>(allNnList.length);
		else
			newQue =new ArrayBlockingQueue<NeighborWritable>(1);
		
		for (int i =0; i < New.size(); i++) {
//			New.get(i).markFlagAsNeutral();
//			New.get(i).markNewAsNeutral();
//			New.get(i).markNewRAsNeutral();
			New.get(i).setRemain(0);
			New.get(i).markSymbolAsFalse();
			
			newQue.put(New.get(i));
		}
		
		for (int i =0; i < Old.size(); i++) {
//			Old.get(i).markFlagAsNeutral();
//			Old.get(i).markNewAsNeutral();
//			Old.get(i).markNewRAsNeutral();
			Old.get(i).setRemain(0);
			Old.get(i).markSymbolAsFalse();
		}
		
		// Local Join
		for (int i =0; i < New.size(); i++) {
			tmpNei  =newQue.poll();
			tmpNode =new NodeWritable(tmpNei.getWritableNode(), tmpNei.getWritablePoint());
			
			tmpNew  =newQue.toArray(new NeighborWritable[newQue.size()]);
			
			for (int j =i + 1, c =0; j < New.size(); j++, c++) {
				dissimNN[i][j] =eucDist(tmpNode.getFloatPoint(), tmpNew[c].getFloatPoint(), context);
				dissimNN[j][i] =dissimNN[i][j];
			}
			
			for (int j =0, c=i + 1; j < tmpNew.length; j++) {
				if (c >= New.size()) c =0;
				
				tmpNew[j].setDist(dissimNN[i][c++]);
			}
			
			newAry.set(tmpNew);
			context.write(tmpNode, newAry);
			
			for (int j =0; j < Old.size(); j++) {
				if (tmpNode.getIntNode() == Old.get(j).getIntNode())
					dissimNO[i][j] =Float.MAX_VALUE;
				else
					dissimNO[i][j] =eucDist(tmpNode.getFloatPoint(), Old.get(j).getFloatPoint(), context);
				
				Old.get(j).setDist(dissimNO[i][j]);
			}
			
			oldAry.set(Old.toArray(new NeighborWritable[Old.size()]));
			context.write(tmpNode, oldAry);
			
			newQue.put(tmpNei);
		}

		tmpNew =newQue.toArray(new NeighborWritable[newQue.size()]);
		for (int i =0; i < Old.size(); i++) {
			tmpNei  =Old.get(i);
			tmpNode =new NodeWritable(tmpNei.getWritableNode(), tmpNei.getWritablePoint());
			
			for (int j =0; j < tmpNew.length; j++)
				 tmpNew[j].setDist(dissimNO[j][i]);
			
			newAry.set(tmpNew);
			
			context.write(tmpNode, newAry);
		}
		
	}
	
	float eucDist(float[] s, float[] t, Context context) {
		context.getCounter(UpdateCounter.CALLDIST).increment(1);
		
		double dist =0.0;
		
		for (int i =0; i < s.length; i++) {
			dist +=(double)s[i]*(double)t[i];
		}
		
		dist =2.0*(1.0 - dist);
		if (dist > 0) dist =Math.sqrt(dist);
		else          dist =0.0;
		
		return (float)dist;
	}
}
