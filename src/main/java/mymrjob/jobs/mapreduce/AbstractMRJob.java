package mymrjob.jobs.mapreduce;

import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class AbstractMRJob extends Configured implements Tool {

	protected static Logger logger = LoggerFactory.getLogger(AbstractMRJob.class);

	/**
	 * Execute the command with the given arguments.
	 *
	 * @param args command specific arguments.
	 * @return exit code.
	 * @throws Exception
	 */
	@Override
	public abstract int run(String[] args) throws Exception;

	/**
	 * 将零散的统计总的run
	 * @param args
	 * @param myJobConf
	 * @return
	 * @throws Exception
	 */
	public int run(String[] args,MyJobConf myJobConf) throws Exception{

		logger.info("\n>>>>>>>>>>>>>>>>>>>>执行任务{}，开始。。。。。。。",myJobConf.getJobname());
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();

		Configuration configuration = getConf();
		Job job = Job.getInstance(configuration, myJobConf.getJobname());

		job.setJarByClass(myJobConf.getJarByClass());
		job.setMapperClass(myJobConf.getMapper());
		job.setReducerClass(myJobConf.getReducer());

		job.setCombinerClass(myJobConf.getCombiner());
		job.setPartitionerClass(myJobConf.getPartitioner());
		job.setSortComparatorClass(myJobConf.getComparator());

		job.setMapOutputKeyClass(myJobConf.getMapOutKey());
		job.setMapOutputValueClass(myJobConf.getMapOutValue());
		job.setOutputKeyClass(myJobConf.getReducerOutKey());
		job.setOutputValueClass(myJobConf.getReducerOutValue());

		//LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		//FileOutputFormat.setCompressOutput(job,true);
		//FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);

		if(HandleType.MULTIPLE_INPUT.equals(myJobConf.getHandleType())){
			this.multipleInputPath(args,job);
		}else{
			this.handlePath(args,job);
		}
		int status = job.waitForCompletion(true) ? 0 : 1;

		stopWatch.stop();
		logger.info("\n>>>>>>>>>>>>>执行任务{}执行所花时间(毫秒):{}",myJobConf.getJobname(),stopWatch.getTime());
		return status;
	}

	/**
	 * 处理参数路径
	 * @param args
	 * @param job
	 */
	public void handlePath(String[] args,Job job) throws IOException {
		Path inputpath = new Path(args[0]);
		Path outputpath = new Path(args[1]);
		FileInputFormat.addInputPath(job,inputpath);
		FileOutputFormat.setOutputPath(job,outputpath);
		Configuration configuration = job.getConfiguration();
		FileSystem fileSystem = FileSystem.get(configuration);
		if(fileSystem.exists(outputpath)){
			fileSystem.delete(outputpath,true);
		}
	}

	/**
	 * 多路径输入
	 * @param args
	 * @param job
	 */
	public void multipleInputPath(String[] args,Job job) throws IOException {
		String inputsPath = args[0];
		Path outputpath = new Path(args[1]);
		FileInputFormat.addInputPaths(job,inputsPath);
		Configuration configuration = job.getConfiguration();
		FileSystem fileSystem = FileSystem.get(configuration);
		if(fileSystem.exists(outputpath)){
			fileSystem.delete(outputpath,true);
		}
		FileOutputFormat.setOutputPath(job,outputpath);
	}

	protected static class MapCombiner extends Reducer <MyWritable,MyWritable,MyWritable,MyWritable>{

		MyWritable valueOut = new MyWritable();
		/**
		 * This method is called once for each key. Most applications will define
		 * their reduce class by overriding this method. The default implementation
		 * is an identity function.
		 * @param key
		 * @param values
		 * @param context
		 */
		@Override
		protected void reduce(MyWritable key, Iterable<MyWritable> values, Context context) throws IOException, InterruptedException {

			logger.info("\ncombiner start ...................");
			long sum = 0;
			for(MyWritable myWritable : values){
				sum += myWritable.getSum();
			}
			valueOut.setSum(sum);
			context.write(key,valueOut);
			logger.info("\ncombiner end ...................");
		}
	}

	protected static class MapPartitioner extends Partitioner<MyWritable,MyWritable>{
		@Override
		public int getPartition(MyWritable key, MyWritable value, int numPartitions) {
			logger.info("\nPartitioner start ...................");
			return key.hashCode()%numPartitions;
		}
	}

	protected static class MapReduceCompare extends WritableComparator{

		public MapReduceCompare() {
			super(MyWritable.class);
		}
		@Override
		public int compare(byte[] b1, int s1, int l1,
		                   byte[] b2, int s2, int l2) {
			int n1 = WritableUtils.decodeVIntSize(b1[s1]);
			int n2 = WritableUtils.decodeVIntSize(b2[s2]);
			return -compareBytes(b1, s1+n1, l1-n1, b2, s2+n2, l2-n2);
		}

		/*@Override
		public int compare(WritableComparable a, WritableComparable b) {
			return super.compare(a, b);
		}*/
	}

}
