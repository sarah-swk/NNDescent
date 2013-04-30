package nndst.rs;

import nndst.serializer.NeighborArrayWritable;
import nndst.serializer.NodeWritable;
import nndst.util.CommonLib;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RSAVTest extends Configured implements Tool 
{	
	private final static int param =4;

	/**
	 * @param args
	 *  args[0]: Input Path
	 *  args[1]: Output Path
	 */
	public static void main(String[] args) throws Exception 
	{
		int      status;
		String[] otherArgs;
		
		Configuration conf;
		
		
		conf      =new Configuration();
		otherArgs =new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length != param) {
			System.out.println("Usage: RSAVTest <Input Path> <Output Path> <K> <RHO>");
			System.exit(-1);
		}
		
		conf.setInt("param.k", Integer.parseInt(otherArgs[2]));
		conf.setFloat("param.rho", Float.parseFloat(otherArgs[3]));
		
		status =ToolRunner.run(conf, new RSAVTest(), otherArgs);
		System.exit(status);
	} //main()
	
	@Override
	public int run(String[] args) throws Exception 
	{
		int status;
		
		Job job =new Job(getConf(), "RSAVTest");
		job.setJarByClass(RSAVTest.class);
		
		job.setMapperClass(RSAVMapper.class);
		job.setMapOutputKeyClass(NodeWritable.class);
		job.setMapOutputValueClass(NeighborArrayWritable.class);
		
		job.setReducerClass(RSAVReducer.class);
		job.setOutputKeyClass(NodeWritable.class);
		job.setOutputValueClass(NeighborArrayWritable.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, new Path(args[0]));
		
		CommonLib.delete(getConf(), new Path(args[1]));
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		status =job.waitForCompletion(true) ? 0 : -1;
		
		return status;
	} //run()

}
