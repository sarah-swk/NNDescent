package nndst.rs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import nndst.serializer.NeighborArrayWritable;
import nndst.serializer.NeighborWritable;
import nndst.serializer.NodeWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;

public class RSAVMapper extends 
	Mapper<NodeWritable, NeighborArrayWritable, NodeWritable, NeighborArrayWritable> 
{
	private int   K;
	private float RHO;
	
	private int sampleSize;
	
	private final int   defaultK   =20;
	private final float defaultRHO =(float) 0.5;
	
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException 
	{
		Configuration conf =context.getConfiguration();
		
		K   =conf.getInt("param.k", defaultK);
		RHO =conf.getFloat("param.rho", defaultRHO);
		
		sampleSize =(int) (RHO*K);
	}
	
	@Override
	protected void map(NodeWritable inkey, NeighborArrayWritable inval, Context context)
		throws IOException, InterruptedException
	{
		NodeWritable          outkey;
		NeighborArrayWritable outval;
		NeighborWritable      identityVal;
		NeighborWritable[]    inAry;
		
		List<NeighborWritable> allNewR;
		List<NeighborWritable> otherR;
		
//		NeighborWritable[] outNewR;
		NeighborWritable[] sampledNR;
//		NeighborWritable[] notSampledNR;
		
		allNewR =new ArrayList<NeighborWritable>();
		otherR  =new ArrayList<NeighborWritable>();
		
		identityVal =new NeighborWritable();
		inAry       =(NeighborWritable[]) inval.toArray();
		
		for (int i =0; i < inAry.length; i++) {
			if (inAry[i].getIntNode() == inkey.getIntNode()) 
				identityVal =inAry[i];
			else if (inAry[i].getIntRemain() == 1) 
				allNewR.add(inAry[i]);
			else 
				otherR.add(inAry[i]);
		}
		
		//TODO: the case allNewR.size() == 0
		if (allNewR.size() > sampleSize)
			sampledNR =getSampledNewR(allNewR.toArray(new NeighborWritable[allNewR.size()]));
		else
			sampledNR =allNewR.toArray(new NeighborWritable[allNewR.size()]);
//		sampledNR    =new NeighborWritable[sampleSize];
//		notSampledNR =new NeighborWritable[allNewR.size() - sampleSize];
//		
//		Random rnd =new Random();
		
		outkey =new NodeWritable(identityVal.getWritableNode(), identityVal.getWritablePoint());
		outval =new NeighborArrayWritable();
		outval.set(sampledNR);
		
		context.write(outkey, outval);
		
		
		NeighborWritable[] tmpAry =new NeighborWritable[1];
		tmpAry[0]                 =identityVal;
		
//		tmpAry[0].markFlagAsTrue();
		tmpAry[0].markSymbolAsTrue();
		tmpAry[0].setRemain(0);
		outval.set(tmpAry);
		
		for (int i =0; i < otherR.size(); i++) 
			context.write(new NodeWritable(otherR.get(i).getWritableNode(), otherR.get(i).getWritablePoint()), outval);
		
//		tmpAry[0].markFlagAsFalse();
//		tmpAry[0].markNewAsTrue();
		tmpAry[0].setRemain(1);
		outval.set(tmpAry);
		
		for (int i =0; i < allNewR.size(); i++) 
			context.write(new NodeWritable(allNewR.get(i).getWritableNode(), allNewR.get(i).getWritablePoint()), outval);
	}
	
	protected NeighborWritable[] getSampledNewR(NeighborWritable[] all) 
	{
		HashSet<NeighborWritable> res =new HashSet<NeighborWritable>(all.length);
		
		Random rnd =new Random();
		
		for (int i =0; i < sampleSize; i++) {
			int r =rnd.nextInt(all.length);
			
			if (res.contains(all[r]))
				{ i--; continue; }
			
			res.add(all[r]);
		}
		
		return res.toArray(new NeighborWritable[res.size()]);
	}
}
