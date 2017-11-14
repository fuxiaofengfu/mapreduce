package mymrjob.jobs.mapreduce;

import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class AbstractMRJob extends Configured implements Tool {

	protected static Logger logger = LoggerFactory.getLogger(AbstractMRJob.class);

	/**
	 * 将零散的统计总的run
	 * @param args
	 * @param
	 * @return
	 * @throws Exception
	 */
	@Override
	public int run(String[] args) throws Exception{
		Job job = this.getJob(args);
		logger.info("\n>>>>>>>>>>>>>>>>>>>>执行任务{}，开始。。。。。。。",job.getJobName());
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		int status = job.waitForCompletion(true) ? 0 : 1;
		stopWatch.stop();
		logger.info("\n>>>>>>>>>>>>>执行任务{}执行所花时间(毫秒):{}",job.getJobName(),stopWatch.getTime());
		return status;
	}

	/**
	 * 获取job
	 * @param args
	 * @param myJobConf
	 * @return
	 * @throws IOException
	 */
	protected Job getJob(String[] args, MyJobConf myJobConf) throws IOException {

		Configuration configuration = getConf();
		if(null == configuration){
			configuration = new Configuration();
		}
		String jobName = this.getJobTaskName(myJobConf.getJobname(),configuration);
		Job job = Job.getInstance(configuration,jobName);
		job.setJarByClass(myJobConf.getJarByClass());
		job.setMapperClass(myJobConf.getMapper());
		job.setReducerClass(myJobConf.getReducer());

		job.setCombinerClass(myJobConf.getCombiner());
		job.setPartitionerClass(myJobConf.getPartitioner());
		job.setSortComparatorClass(myJobConf.getComparator());

		job.setGroupingComparatorClass(myJobConf.getGroupComparator());
		job.setMapOutputKeyClass(myJobConf.getMapOutKey());
		job.setMapOutputValueClass(myJobConf.getMapOutValue());
		job.setOutputKeyClass(myJobConf.getReducerOutKey());
		job.setOutputValueClass(myJobConf.getReducerOutValue());

		//LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		//FileOutputFormat.setCompressOutput(job,true);
		//FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
		this.handlePath(args,job);
		Path outputpath = FileOutputFormat.getOutputPath(job);
		FileSystem fileSystem = FileSystem.get(configuration);
		if(fileSystem.exists(outputpath)){
			fileSystem.delete(outputpath,true);
		}

		return job;
	}

	protected abstract Job getJob(String args[]) throws Exception;
	/**
	 * 处理参数路径
	 * @param args
	 * @param job
	 */
	protected abstract void handlePath(String[] args,Job job) throws IOException;

	/**
	 * 获取任务id
	 * @return
	 */
	protected  String getJobTaskName(String jobName,Configuration configuration){
		String frameWorkName = configuration.get(MRConfig.FRAMEWORK_NAME,MRConfig.LOCAL_FRAMEWORK_NAME);
		StringBuilder builder = new StringBuilder(jobName);
		builder.append("_").append(frameWorkName).append("_").append(System.currentTimeMillis());
		return builder.toString();
	}

	protected static class MapPartitioner extends Partitioner<MyWritable,MyWritable>{
		@Override
		public int getPartition(MyWritable key, MyWritable value, int numPartitions) {
			logger.info("\nPartitioner start ...................");

			//super.getPartition(key,value,numPartitions);
			int partition = key.hashCode() % numPartitions;
			return partition;
		}
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

			//logger.info("\ncombiner start ...................");
			long sum = 0;
			for(MyWritable myWritable : values){
				sum += myWritable.getSum();
			}
			valueOut.setSum(sum);
			context.write(key,valueOut);
			//logger.info("\ncombiner end ...................");
		}
	}

	protected static class MapReduceCompare extends WritableComparator{

		public MapReduceCompare() {
			super(MyWritable.class,true);
		}
		/*@Override
		public int compare(byte[] b1, int s1, int l1,
		                   byte[] b2, int s2, int l2) {
			int n1 = WritableUtils.decodeVIntSize(b1[s1]);
			int n2 = WritableUtils.decodeVIntSize(b2[s2]);
			return -compareBytes(b1, s1+n1, l1-n1, b2, s2+n2, l2-n2);
		}*/

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			//System.out.println("myWritable extends comparator .........");
			return super.compare(a,b);
		}
	}

}
