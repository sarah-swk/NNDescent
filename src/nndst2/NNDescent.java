package nndst;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import nndst.lj.LJMapper;
import nndst.lj.LJReducer;
import nndst.pp.PPMapper;
import nndst.pp.PPReducer;
import nndst.rs.RSAVMapper;
import nndst.rs.RSAVReducer;
import nndst.rs.RSMapper;
import nndst.rs.RSReducer;
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

public class NNDescent extends Configured implements Tool
{
	private final static int param      =7;
	private final static int pathLength =3;
	
	private final static int ppIn       =0;
	private final static int ppOut      =1;
	private final static int ljOut      =2;
	
	private final static String ppBase  ="depth_";
	private final static String ljBase  ="ljdata_";
	
	private static int    N;
	private static int    K;
	private static float  RHO;
	private static float  SIGMA;
	
	private static int    iteration;
	private static long   counter;
	private static long   callDist;
	
	private static String resfile;
	private static String outtext;
	
	/**
	 * @param args
	 *  args[0]: Base Path
	 *  args[1]: Data Path
	 *  args[2]: N
	 *  args[3]: K
	 *  args[4]: RHO
	 *  args[5]: SIGMA
	 */
	public static void main(String[] args) throws Exception
	{
		int status;
		
		long startTime;
		long stopTime;
		
		long min;
		long sec;
		long diffSec;

		String[]      otherArgs;
		
		Configuration conf;
		
		
		conf      =new Configuration();
		otherArgs =new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length != param) {
			System.out.println("Usage: Mkannlist <Base Path> <Data Path> <N> <K> <RHO> <SIGMA> <Res Path>");
			System.exit(-1);
		}
		
		iteration =0;
		callDist  =0;
		
		N       =Integer.parseInt(otherArgs[2]);
		K       =Integer.parseInt(otherArgs[3]);
		RHO     =Float.parseFloat(otherArgs[4]);
		SIGMA   =Float.parseFloat(otherArgs[5]);
		resfile =otherArgs[6];
		
		outtext ="#### Start: N = " + N + " ##########" + "\n";
		
		conf.setInt("param.n", N);
		conf.setInt("param.k", K);
		conf.setFloat("param.rho", RHO);
		
		//===== Run the process =====================================
		
		startTime =System.currentTimeMillis();
		status    =ToolRunner.run(conf, new NNDescent(), otherArgs);
		stopTime  =System.currentTimeMillis();
		
		//===== End the process =====================================
		
		diffSec =(stopTime - startTime)/1000;
		min     =diffSec/60;
		sec     =diffSec - min*60;
		
		System.out.println("Time: " + min + " min " + sec + " sec ");
		outtext +=("Time: " + min + " min " + sec + " sec " + "\n\n\n");
		
		FileWriter     outFile   =new FileWriter(resfile);
		BufferedWriter outBuffer =new BufferedWriter(outFile);
		
		outBuffer.write(outtext);
		outBuffer.flush();
		outBuffer.close();
		
		System.exit(status);
		
	} //main()
	
	@Override
	public int run(String[] args) throws Exception 
	{
		int ET;
		
		boolean isSuccess;
		
		String  basePath;
		String  dataPath;
		
		Path    rsIn;
		Path    rsOut;
		
		Path[]  pathList;
		
		Job     rsJob;
		Job     rsajJob;
		Job     ppJob;
		Job     ljJob;
		
		
		ET =(int)(SIGMA*K*N);
		
		basePath =args[0];
		dataPath =args[1];
		
		//===== Job 0: Conduct a Random Sampling ====================
		
		rsIn      =new Path(dataPath);
		rsOut     =getOutPath(basePath);
		
		rsJob     =createRSJob(rsIn, rsOut);
		isSuccess =rsJob.waitForCompletion(true);
		
		if (!isSuccess) return -1;

		
		iteration++;
		
		pathList  =getPathList(basePath);
		rsajJob   =createRsAdjJob(pathList[ppIn], pathList[ppOut]);
		
		isSuccess =rsajJob.waitForCompletion(true);
		
		if (!isSuccess) return -1;
		else            CommonLib.delete(getConf(), pathList[ppIn]);
		
		ljJob     =createLocalJoin(pathList[ppOut], pathList[ljOut]);
		isSuccess =ljJob.waitForCompletion(true);
		
		if (!isSuccess) return -1;
		else            CommonLib.delete(getConf(), pathList[ppOut]);
		
		counter  =ljJob.getCounters().findCounter(LJReducer.UpdateCounter.CONVERGED).getValue();
		callDist +=ljJob.getCounters().findCounter(LJMapper.UpdateCounter.CALLDIST).getValue();

		System.out.println("iter: " + iteration + "\tcallDist: " 
				+ callDist + "\tconveged: " + counter);
		outtext +=("iter: " + iteration + "\tcallDist: " + callDist + "\tconveged: " + counter + "\n");
		
		//====== End of the job 0 ===================================
		
		while (counter > ET && isSuccess) {
			iteration++;
			pathList =getPathList(basePath);
			
			//=== Job 1: Create an Adjacency vertex set for local join ===
			
			ppJob     =createPreProcess(pathList[ppIn], pathList[ppOut]);
			isSuccess =ppJob.waitForCompletion(true);
			
			if (!isSuccess) return -1;
			else            CommonLib.delete(getConf(), pathList[ppIn]);
			
			//====== End of the job1 =====================================
			
			//===== Job 2: Conduct a Local Join =========================
			
			ljJob     =createLocalJoin(pathList[ppOut], pathList[ljOut]);
			isSuccess =ljJob.waitForCompletion(true);
			
			if (!isSuccess) return -1;
			else            CommonLib.delete(getConf(), pathList[ppOut]);
			
			//====== End of the job2 ====================================
			
			counter  =ljJob.getCounters().findCounter(LJReducer.UpdateCounter.CONVERGED).getValue();
			callDist +=ljJob.getCounters().findCounter(LJMapper.UpdateCounter.CALLDIST).getValue();

			System.out.println("iter: " + iteration + "\tcallDist: " 
					+ callDist + "\tconveged: " + counter);
			outtext +=("iter: " + iteration + "\tcallDist: " + callDist + "\tconveged: " + counter + "\n");
		}
		
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

	//===== Job 0-1: RSJob ======================================
	public Job createRSJob(Path in, Path out) throws IOException 
	{
		Job job =new Job(getConf(), "RandomSampling");
		job.setJarByClass(NNDescent.class);
			
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
	
	//===== Job 0-2: rsajJob ======================================
	protected Job createRsAdjJob(Path in, Path out) throws IOException {
		Job job =new Job(getConf(), "RSAVTest " + iteration);
		job.setJarByClass(NNDescent.class);
			
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
	} //createRsAdjJob()
	
	//===== Job 1: PreProcessing ================================
	protected Job createPreProcess(Path in, Path out) throws IOException {
		Job job =new Job(getConf(), "Pre Processing " + iteration);
		job.setJarByClass(NNDescent.class);
		
		job.setMapperClass(PPMapper.class);
		job.setMapOutputKeyClass(NodeWritable.class);
		job.setMapOutputValueClass(NeighborArrayWritable.class);
		
		job.setReducerClass(PPReducer.class);
		job.setOutputKeyClass(NodeWritable.class);
		job.setOutputValueClass(NeighborArrayWritable.class);
			
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
			
		CommonLib.delete(getConf(), out);
			
		SequenceFileInputFormat.setInputPaths(job, in);
		SequenceFileOutputFormat.setOutputPath(job, out);
			
		return job;
	}
	
	//===== Job 2: Local Join ===================================
		protected Job createLocalJoin(Path in, Path out) throws IOException {
			Job job  =new Job(getConf(), "local join" + iteration);
			job.setJarByClass(NNDescent.class);
			
			job.setMapperClass(LJMapper.class);
			job.setMapOutputKeyClass(NodeWritable.class);
			job.setMapOutputValueClass(NeighborArrayWritable.class);
			
//			job.setCombinerClass(LJCombine.class);
			
			job.setReducerClass(LJReducer.class);
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
