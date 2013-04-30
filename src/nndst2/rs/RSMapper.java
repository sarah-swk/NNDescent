package nndst.rs;

import java.io.IOException;
import java.util.Random;

import nndst.serializer.FloatArrayWritable;
import nndst.serializer.NeighborWritable;
import nndst.serializer.NodeWritable;
import nndst.util.CommonLib;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class RSMapper extends 
	Mapper<IntWritable, FloatArrayWritable, NodeWritable, NeighborWritable>{
	
	private int   K;
	private int   N;
	private float RHO;
	
	private int   sampleSize;
	
	private final int   defaultK   =20;
	private final int   defaultN   =10000;
	private final float defaultRHO =(float) 0.5;
	
	private final int bdrecords[] ={ 12939827, 12949810, 12959799, 38075869, 38084444, 38093278, 38094117, 38150900, 64498412, 64508315 };
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		
		Configuration conf =context.getConfiguration();
		
		K          =conf.getInt("param.k", defaultK);
		N          =conf.getInt("param.n", defaultN);
		RHO        =conf.getFloat("param.rho", defaultRHO);
		
		sampleSize =(int) (RHO*K);
	}
	
	
	@Override
	protected void map(IntWritable inkey, FloatArrayWritable inval, Context context)
		throws IOException, InterruptedException {
		
		NodeWritable     outkey;
		NeighborWritable identityVal;
		NodeWritable[]   knnl;
		
		outkey      =new NodeWritable(inkey.get());
		identityVal =new NeighborWritable(inkey, inval, new FloatWritable(Float.MAX_VALUE));
		knnl        =sampleFromAllItems(inkey.get());
		
		context.write(outkey, identityVal);
		identityVal.markSymbolAsFalse();
		
		for (int i =0; i < K; i++) {
			if (knnl[i].isFlagTrue()) identityVal.setRemain(1);
			else                      identityVal.setRemain(0);
			
			context.write(knnl[i], identityVal);
		}
	}
	
	protected NodeWritable[] sampleFromAllItems(int except) {
		
		NodeWritable[] knnl =new NodeWritable[K];
		Random         rnd  =new Random();
		
		
		for (int i =0; i < K; i++) {
			int r =rnd.nextInt(N);
			
			if (r == except || CommonLib.contain(knnl, r) || CommonLib.contain(bdrecords, r)) i--;
			else                                           knnl[i] =new NodeWritable(r);
		}
		
		for (int i =0; i < sampleSize; i++) {
			int r =rnd.nextInt(K);
			
			if (knnl[r].isFlagTrue())
				{ i--; continue; }
			else knnl[r].markFlagAsTrue();
		}
		
		return knnl;
	}
	
//	protected boolean contain(NodeWritable[] ary, int len, int key) {
//		boolean res =false;
//		
//		for (int i =0; i < len; i++)
//			if (ary[i].getIntNode() == key)
//				{ res =true; break; }
//		
//		return res;
//	}
}
