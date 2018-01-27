package mymrjob.jobs;

import mymrjob.jobs.mapreduce.AbstractMRJob;
import mymrjob.jobs.mapreduce.MyJobConf;
import mymrjob.jobs.mapreduce.MyWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 多目录输入及多目录输出及排序
 */
public class SortWordJob extends AbstractMRJob {

	private static final String SPLIT_REGEX="=====";

	@Override
	public Job getJob(String[] args) throws Exception {

		MyJobConf myJobConf = new MyJobConf("sortWordJob",SortWordJob.class,SortWordJobReducer.class,SortWordJobMapper.class);
		myJobConf.setGroupComparator(SortWordJobGroupComparator.class);
		myJobConf.setPartitioner(SortWordJobPartitioner.class);
		//因为这里只是排序，不需要求和，所以这里不能使用默认的combiner
		myJobConf.setCombiner(Reducer.class);
		return super.getJob(args,myJobConf);
	}

	/**
	 * 处理参数路径
	 *
	 * @param args
	 * @param job
	 */
	@Override
	public void handlePath(String[] args, Job job) throws IOException {
		FileInputFormat.setInputDirRecursive(job,true);
		FileInputFormat.setInputPaths(job,args[0]);
		//FileInputFormat.addInputPath(job,inputPath);
		Path outPutPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job,outPutPath);

		String[] input1 = args[0].split(",");

		MultipleInputs.addInputPath(job,new Path(input1[0]), TextInputFormat.class,SortWordJobMapper.class);
		MultipleInputs.addInputPath(job,new Path(input1[1]), TextInputFormat.class,SortWordJobMapper.class);
		//MultipleOutputs.addNamedOutput(job,"10aaa", TextOutputFormat.class,Text.class,LongWritable.class);
		//MultipleOutputs.addNamedOutput(job,"10cc", TextOutputFormat.class,Text.class,LongWritable.class);
		//MultipleOutputs.addNamedOutput(job,"101vb", TextOutputFormat.class,Text.class,LongWritable.class);
	}

	private static class SortWordJobMapper extends Mapper<LongWritable,Text,MyWritable,MyWritable>{

		MyWritable kyout = new MyWritable();
		MyWritable vlout = new MyWritable();
		/**
		 * Called once at the beginning of the task.
		 *
		 * @param context
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			System.out.println("map start ...........");
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] values = value.toString().split("\t");

            if(values.length <=1){
            	context.getCounter("myCounter","bad lines num =="+value).increment(1);
            	return;
            }

            String txtkey = values[0];
            String txtValue = values[1];
			kyout.setValue(txtkey+"===="+txtValue);
            vlout.setSum(Long.parseLong(txtValue));
            /*vlout.setValue(values[0]);
			kyout.setSum(Long.parseLong(values[1]));*/
			context.write(kyout,vlout);
		}

		/**
		 * Called once at the end of the task.
		 *
		 * @param context
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			System.out.println("map end .......................");
		}
	}

	private static class SortWordJobMapper2 extends Mapper<LongWritable,Text,MyWritable,MyWritable>{

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//			super.map(key, value, context);
		}
	}

	private static class SortWordJobPartitioner extends Partitioner<MyWritable,MyWritable> {
		@Override
		public int getPartition(MyWritable key, MyWritable value, int numPartitions) {
			logger.info("\nPartitioner start ...................");
			String txtKey = key.getValue().split("====")[0];

			MyWritable myWritable = new MyWritable();
			myWritable.setValue(txtKey);
            int hashCode = myWritable.hashCode();
			int partition = hashCode % numPartitions;
			return 0;
		}
	}

	private static class SortWordJobReducer extends Reducer<MyWritable,MyWritable,Text,LongWritable>{
		//MultipleOutputs outputs;

		Text keyOut = new Text();
		LongWritable valueOut = new LongWritable();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			//outputs = new MultipleOutputs(context);
			logger.info("reduce start ..............................");
		}

		@Override
		protected void reduce(MyWritable key, Iterable<MyWritable> values, Context context) throws IOException, InterruptedException {

			for(MyWritable v : values){
				keyOut.set(key.getValue());
				//keyOut.set(v.getValue());
				valueOut.set(v.getSum());
				context.write(keyOut,valueOut);
			}
			//outputs.write("10aaa",keyOut,valueOut,"o1");
			//outputs.write("10cc",keyOut,valueOut,"o2");
			//outputs.write("101vb",keyOut,valueOut,"o3");
			System.out.println("reduce ************************");
		}

		/**
		 * Called once at the end of the task.
		 *
		 * @param context
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			//outputs.close();
			System.out.println("reduce end *************************");
		}
	}

	private static class SortWordJobGroupComparator extends WritableComparator {

		public SortWordJobGroupComparator() {
			//这儿是map阶段输出的key的类型
			super(MyWritable.class,true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {

			MyWritable a1 = (MyWritable) a;
			MyWritable b1 = (MyWritable) b;
			String[] arr1 = a1.getValue().split("====");
			String[] arr2 = b1.getValue().split("====");
			if(arr1[0].equals(arr2[0])){
				long num1 = Long.parseLong(arr1[1]);
				long num2 = Long.parseLong(arr2[1]);
				if(num1 > num2){
					return -1;
				}
				if(num1 < num2){
					return -1;
				}
				return 0;
			}
			MyWritable myWritable1 = new MyWritable(arr1[0]);
			MyWritable myWritable2 = new MyWritable(arr2[0]);
			return arr1[0].compareTo(arr1[0]);
		}
	}
}
