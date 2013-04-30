package nndst.eval;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class EvalDong extends Configured implements Tool {

	public static class EvalMapper 
			extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		
		protected void map(LongWritable inkey, Text inval, Context context) 
				throws IOException, InterruptedException {
			String[] line =inval.toString().split(" ");
			
			context.write(new IntWritable(Integer.parseInt(line[0])), 
						new IntWritable(Integer.parseInt(line[1])));
		}
	}
	
	public static class EvalReducer
			extends Reducer<IntWritable, IntWritable, IntWritable, DoubleWritable> {
		private int K;
		private int N;
		private int totalSuc;
		
		private int    defaultK    =20;
		private int    defaultN    =1000;
		private String defaultPath ="C:\\cygwin\\home\\warashina\\hadoop\\dong\\TrueData\\sph.1K.nnsk";
		
		private Map<Integer, int[]> TrueData;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			
			Configuration conf =context.getConfiguration();
			
			K =conf.getInt("param.k", defaultK);
			N =conf.getInt("param.n", defaultN);
			totalSuc =0;
			TrueData =getTrueData(conf);
		}
		
		private Map<Integer, int[]> getTrueData(Configuration conf)	
				throws IOException {
			int counter =0;
			int[] trueKnnL =new int[K];
			
			String line;
			String[] elem;
			
			Map<Integer, int[]> res =new HashMap<Integer, int[]>();
			
			Path truePath =new Path(conf.get("truedata.path", defaultPath));
			FileSystem fs =FileSystem.get(conf);
			
			InputStream in =fs.open(truePath);
			BufferedReader reader =new BufferedReader(new InputStreamReader(in));
			
			try {
				while((line =reader.readLine()) != null) {
					elem =line.split(" ");
					
					trueKnnL[counter] =Integer.parseInt(elem[1]);
					
					if (counter == (K - 1)) {
						res.put(Integer.parseInt(elem[0]), trueKnnL.clone());
						
						counter =0;
						trueKnnL =new int[K];
					} else {
						counter++;
					} //if(counter)
					
				} //while(line)
			} finally {
				in.close();
			}
			
			return res;
		}
		
		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> invals, Context context) 
				throws IOException, InterruptedException {
			int keynode =key.get();
			int suc  =0;
			double recall;
			
			for (IntWritable inval : invals) {
				if (isTrueData(keynode, inval.get())) suc++;
			}
			
			totalSuc +=suc;
			recall =(double)suc/K;
			context.write(key, new DoubleWritable(recall));
		}
		
		boolean isTrueData(int keynode, int valnode) {
			boolean res =false;
			int[] knnL =TrueData.get(keynode);
			
			for (int i =0; i < knnL.length; i++) {
				if (knnL[i] == valnode) 
					{ res =true; break; }
			}
			
			return res;
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
			
			double allKnn =N*K;
			double scanrate =(double)totalSuc/allKnn;
			
			context.write(new IntWritable(-1), new DoubleWritable(scanrate));
		}
	}
	
	/**
	 * @param args
	 *  args[0]: Input Data Path
	 *  args[1]: Result Output Path
	 *  args[2]: True Data Path
	 *  args[3]: K
	 *  args[4]: N
	 */
	
	private final static int param    =5;
	private final static int trueData =2;
	private final static int k        =3;
	private final static int n        =4;
	
	public static void main(String[] args) throws Exception {
		Configuration conf =new Configuration();
		String[] otherArgs =new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length != param) {
			System.out.println("Usage: EvalDong <In> <Out> <True> <K> <N>");
			System.exit(-1);
		}
		
		conf.set("truedata.path", otherArgs[trueData]);
		conf.setInt("param.k", Integer.parseInt(otherArgs[k]));
		conf.setInt("param.n", Integer.parseInt(otherArgs[n]));
		
		int status =ToolRunner.run(conf, new EvalDong(), otherArgs);
		System.exit(status);
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf =getConf();
		
		Path in  =new Path(args[0]);
		Path out =new Path(args[1]);
		
		Job job =new Job(conf, "EvalDong");
		job.setJarByClass(EvalDong.class);
		
		job.setMapperClass(EvalMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(EvalReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job, in);
		
		delete(conf, out);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, out);
		
		return job.waitForCompletion(true) ? 0 : -1;
	}
	
	private static void delete(Configuration conf, Path out) throws IOException {
		FileSystem fs =FileSystem.get(conf);
		
		if (fs.exists(out)) fs.delete(out, true);
		
		return;
	}

}
