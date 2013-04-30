package nndst.util;

import java.io.File;
import java.io.IOException;

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


public class SFNnReader extends Configured implements Tool {
	
	public static class SFMapper 
			extends Mapper<NodeWritable, NeighborWritable, IntWritable, NeighborWritable> {
		
		@Override
		protected void map(NodeWritable inkey, NeighborWritable inval, Context context)
				throws IOException, InterruptedException {
			NeighborWritable outval =new NeighborWritable(new IntWritable(inkey.getIntNode() + 1), inkey.getWritablePoint(), inval.getWritableDist());
			
			if (!inval.getBooleanSymbol()) outval.markSymbolAsFalse();

			context.write(new IntWritable(inval.getIntNode() + 1), outval);
		}
	}
	
	public static class SFReducer
			extends Reducer<IntWritable, NeighborWritable, IntWritable, Text> {
		
		@Override
		protected void reduce(IntWritable key, Iterable<NeighborWritable> invals, Context context) 
				throws IOException, InterruptedException {
			String symbol, dist, outtext;
			
			// Value is not sorted -> Secondary Sort should be implemented
			for (NeighborWritable inval : invals) {
				outtext ="";
				
				dist   =String.format("%e", inval.getFloatDist());
				symbol =inval.getBooleanSymbol() ? "B" : "RT";
				
				outtext +=inval.getIntNode() + "\t" + dist + "\t" + symbol;
				
//				if (inval.isNewRTrue()) outtext +="NEWR = True";
//				else                    outtext +="NEWR = Neutral";
				
				context.write(key, new Text(outtext));
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf =new Configuration();
		
		conf.set("mapred.textoutputformat.separator", " ");
		int status =ToolRunner.run(conf, new SFNnReader(), args);
		System.exit(status);
	}
	
	public int run(String[] args) throws Exception {
		Job job =new Job(getConf(), "SFKnnReader");
		job.setJarByClass(SFNnReader.class);
		
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
