package nndst.util;

import java.io.File;
import java.io.IOException;

import nndst.serializer.NeighborArrayWritable;
import nndst.serializer.NeighborWritable;
import nndst.serializer.NodeWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class SFKnnReader extends Configured implements Tool {
	
	public static class SFMapper 
			extends Mapper<NodeWritable, NeighborArrayWritable, IntWritable, NeighborWritable> {
		
		@Override
		protected void map(NodeWritable inkey, NeighborArrayWritable inval, Context context)
				throws IOException, InterruptedException 
		{
			NeighborWritable[] knnL =(NeighborWritable[])inval.toArray();
			
			for (int i =0; i < knnL.length; i++) {
				context.write(new IntWritable(inkey.getIntNode() + 1), knnL[i]);
			}
		}
	}
	
	public static class SFReducer
			extends Reducer<IntWritable, NeighborWritable, IntWritable, Text> {
		
		@Override
		protected void reduce(IntWritable key, Iterable<NeighborWritable> invals, Context context) 
				throws IOException, InterruptedException 
		{	
			String symbol, dist, outtext;
			
			// Value is not sorted -> Secondary Sort should be implemented
			for (NeighborWritable inval : invals) {
				outtext ="";
				
				symbol =inval.getBooleanSymbol() ? "B" : "RT";
				dist =String.format("%e", inval.getFloatDist());
				
				outtext +=(inval.getIntNode() + 1) + " " + dist + " " + inval.getIntRemain() + " " + symbol;
				
//				if (inval.isFlagTrue())         outtext +="FLAG = T ";
//				else if (inval.isFlagNeutral()) outtext +="FLAG = N ";
//				else                            outtext +="FLAG = F ";
//				 
//				if (inval.isNewTrue())          outtext +="NEW = T ";
//				else if (inval.isNewNeutral())  outtext +="NEW = N ";
//				else                            outtext +="NEW = F ";
//				
//				if (inval.isNewRTrue())         outtext +="NEWR = T ";
//				else if (inval.isNewRNeutral()) outtext +="NEWR = N ";
//				else                            outtext +="NEWR = F ";
				
				context.write(key, new Text(outtext));
			}
		}
	}
	
	public static void main(String[] args) throws Exception 
	{
		Configuration conf =new Configuration();
		
		conf.set("mapred.textoutputformat.separator", " ");
		int status =ToolRunner.run(conf, new SFKnnReader(), args);
		System.exit(status);
	}
	
	public int run(String[] args) throws Exception 
	{
		Job job =new Job(getConf(), "SFKnnReader");
		job.setJarByClass(SFKnnReader.class);
		
		job.setMapperClass(SFMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(NeighborWritable.class);
		
//		job.setNumReduceTasks(0);
		job.setReducerClass(SFReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, new Path(args[0]));
		
		CommonLib.delete(new File(args[1]));
		
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : -1;
	}

}
