package nndst.util;

import java.io.IOException;

import nndst.serializer.FloatArrayWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MkSeqFile extends Configured implements Tool {

	public static class SFMapper 
			extends Mapper<LongWritable, Text, IntWritable, FloatArrayWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] line =value.toString().split(" ");
			IntWritable     node      =new IntWritable(Integer.parseInt(line[0]));
			
			float[] tmpAry1 =new float[line.length - 1];
			for (int i =1; i < line.length; i++) {
				tmpAry1[i - 1] =Float.parseFloat(line[i]);
			}
			
			FloatWritable[]    tmpAry2 = new FloatWritable[tmpAry1.length];
			FloatArrayWritable outval  = new FloatArrayWritable();
			
			for (int i =0; i < tmpAry1.length; i++) {
				tmpAry2[i] =new FloatWritable(tmpAry1[i]);
			}

			outval.set(tmpAry2);
			context.write(node, outval);
		}
	}
	
	public static class SFReducer
			extends Reducer<IntWritable, FloatArrayWritable, IntWritable, FloatArrayWritable> {

		@Override
		protected void reduce(IntWritable key, Iterable<FloatArrayWritable> values, 
				Context context) throws IOException, InterruptedException {
			for (FloatArrayWritable d : values) {
				context.write(key, d);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		int status =ToolRunner.run(new Configuration(), new MkSeqFile(), args);
		System.exit(status);
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf =getConf();
		
		Path in  =new Path(args[0]);
		Path out =new Path(args[1]);
		
		Job job =new Job(conf, "MkSeqFile");
		
		job.setJarByClass(MkSeqFile.class);
		job.setMapperClass(SFMapper.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(FloatArrayWritable.class);
		
		job.setReducerClass(SFReducer.class);
//		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.setInputPaths(job, in);
		
		CommonLib.delete(conf, out);
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, out);
//		SequenceFileOutputFormat.setCompressOutput(job, true);
//		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
//		SequenceFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
		
		return job.waitForCompletion(true) ? 0 : 1;		
	}

}