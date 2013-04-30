package nndst.util;

import java.io.File;
import java.io.IOException;

import nndst.serializer.FloatArrayWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SFReader extends Configured implements Tool {

	public static class SFReadMapper extends
			Mapper<IntWritable, FloatArrayWritable, IntWritable, Text> {

		protected void map(IntWritable key, FloatArrayWritable value,
				Context context) throws IOException, InterruptedException 
		{
			FloatWritable[] vector = (FloatWritable[]) value.toArray();

			String outval = "";
			for (int i = 0; i < vector.length; i++) {
				outval += (vector[i].toString() + " ");
			}
			Text val = new Text(outval);
			context.write(key, val);
		}
	
	}
	
	public int run(String[] args) throws Exception {
		Job job =new Job(getConf(), "SFRead");
		job.setJarByClass(SFReader.class);
		
		job.setMapperClass(SFReadMapper.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(0);
		
		SequenceFileInputFormat.setInputPaths(job, new Path(args[0]));
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		CommonLib.delete(new File(args[1]));
		
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int status =ToolRunner.run(new Configuration(), new SFReader(), args);
		System.exit(status);
	}

}
