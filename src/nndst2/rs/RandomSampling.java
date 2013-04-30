package nndst.rs;

import java.io.IOException;

import nndst.serializer.NeighborArrayWritable;
import nndst.serializer.NeighborWritable;
import nndst.serializer.NodeWritable;
import nndst.util.CommonLib;
import nndst.util.SystemUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RandomSampling extends Configured implements Tool 
{	
	private final static int param      =5;
	private final static int pathLength =3;
	
	private final static int ppIn       =0;
	private final static int ppOut      =1;
	private final static int ljOut      =2;
	
	private final static String ppBase  ="depth_";
	private final static String ljBase ="ljdata_";
	
	private static int          iteration;
	

	public static void main(String[] args) throws Exception 
	{	
		int       status;
		String[]  otherArgs;
		
		Configuration conf;
		
		conf      =new Configuration();
		otherArgs =new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length != param) {
			System.out.println("Usage: RandomSampling <Base Path> <Data Path> <N> <K> <RHO>");
			System.exit(-1);
		}
		
		iteration =0;

		conf.setInt("param.n", Integer.parseInt(otherArgs[2]));
		conf.setInt("param.k", Integer.parseInt(otherArgs[3]));
		conf.setFloat("param.rho", Float.parseFloat(otherArgs[4]));
		
		status =ToolRunner.run(conf, new RandomSampling(), otherArgs);
		System.exit(status);
		
	} //main()

	@Override
	public int run(String[] args) throws Exception 
	{
		boolean isSuccess;
		
		Job    rsJob;
		Job    rsAdjJob;
		
		Path   randIn;
		Path   randOut;
		
		Path[] pathList;
		
		
		//===== Job 1: Conduct random sampling ===================
		
		randIn  =new Path(args[1]);
		randOut =getOutPath(args[0]);
		
		rsJob     =createRSJob(randIn, randOut);
		isSuccess =rsJob.waitForCompletion(true);
		
		if (!isSuccess) return -1;
		// else CommonLib.delete(getConf(), new Path(args[1]));
		
		//====== End of the job 1 ===================================

		iteration++;
		
		pathList =getPathList(args[0]);
		rsAdjJob =createRsAdjJob(pathList[ppIn], pathList[ppOut]);
		
		isSuccess =rsAdjJob.waitForCompletion(true);
		
		return isSuccess ? 0 : -1;
	} //run()
	
	public Path getOutPath(String base) 
	{
		String sep;
		
		if (SystemUtils.IS_OS_WINDOWS) sep ="\\";
		else                           sep ="/";
		
		if (!(base.lastIndexOf(sep) == (base.length() - 1)))
			base +=sep;
		
		return new Path(base + ppBase + iteration);
	}
	
	public Path[] getPathList(String basePath) 
	{
		Path[] pathList =new Path[pathLength];
		String sep;
		
		if (SystemUtils.IS_OS_WINDOWS) sep ="\\";
		else                           sep ="/";
		
		if (!(basePath.lastIndexOf(sep) == (basePath.length() - 1)))
			basePath +=sep;
		
		pathList[ppIn]  =new Path(basePath + ppBase + (iteration - 1) + sep);
		pathList[ppOut] =new Path(basePath + ljBase + (iteration - 1));
		pathList[ljOut] =new Path(basePath + ppBase + iteration);
		
		return pathList;
	}
	
	//===== Job 1: RS ===========================================
	public Job createRSJob(Path in, Path out) throws IOException {
		Job job =new Job(getConf(), "RandomSampling");
		job.setJarByClass(RandomSampling.class);
		
		job.setMapperClass(RSMapper.class);
		job.setMapOutputKeyClass(NodeWritable.class);
		job.setMapOutputValueClass(NeighborWritable.class);
		
		job.setReducerClass(RSReducer.class);
		job.setOutputKeyClass(NodeWritable.class);
		job.setOutputValueClass(NeighborArrayWritable.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, in);
		
		CommonLib.delete(getConf(), out);
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, out);
		
		return job;
		
	} //createRSJob()
	
	//===== Job 2: RsAdj ========================================
	protected Job createRsAdjJob(Path in, Path out) throws IOException {
		Job job =new Job(getConf(), "RSAVTest " + iteration);
		job.setJarByClass(RandomSampling.class);
		
		job.setMapperClass(RSAVMapper.class);
		job.setMapOutputKeyClass(NodeWritable.class);
		job.setMapOutputValueClass(NeighborArrayWritable.class);
		
		job.setReducerClass(RSAVReducer.class);
		job.setOutputKeyClass(NodeWritable.class);
		job.setOutputValueClass(NeighborArrayWritable.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		CommonLib.delete(getConf(), out);
		
		SequenceFileInputFormat.setInputPaths(job, in);
		SequenceFileOutputFormat.setOutputPath(job, out);
		
		return job;
	}
	
}
