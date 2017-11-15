package mymrjob.jobs;

import mymrjob.jobs.mapreduce.AbstractMRJob;
import mymrjob.jobs.mapreduce.MyJobConf;
import mymrjob.jobs.mapreduce.MyWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * 多目录输入及多目录输出及排序
 */
public class SortWordJob extends AbstractMRJob {

	@Override
	public Job getJob(String[] args) throws Exception {

		MyJobConf myJobConf = new MyJobConf("sortWordJob",SortWordJob.class,SortWordJobReducer.class,SortWordJobMapper.class);
		myJobConf.setGroupComparator(SortWordJobGroupComparator.class);
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

		MultipleOutputs.addNamedOutput(job,"10aaa", TextOutputFormat.class,Text.class,LongWritable.class);
		MultipleOutputs.addNamedOutput(job,"10cc", TextOutputFormat.class,Text.class,LongWritable.class);
		MultipleOutputs.addNamedOutput(job,"101vb", TextOutputFormat.class,Text.class,LongWritable.class);
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
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] values = value.toString().split("\t");
            kyout.setValue(values[0]);
            vlout.setSum(Long.parseLong(values[1]));
			context.write(kyout,vlout);
		}
	}

	private static class SortWordJobMapper2 extends Mapper<LongWritable,Text,MyWritable,MyWritable>{

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//			super.map(key, value, context);
		}
	}


	private static class SortWordJobReducer extends Reducer<MyWritable,MyWritable,Text,LongWritable>{
		MultipleOutputs outputs;

		Text keyOut = new Text();
		LongWritable valueOut = new LongWritable();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			outputs = new MultipleOutputs(context);
		}

		@Override
		protected void reduce(MyWritable key, Iterable<MyWritable> values, Context context) throws IOException, InterruptedException {

			long sum = 0;
			for(MyWritable v : values){
				sum += v.getSum();
			}
			valueOut.set(sum);
			keyOut.set(key.getValue());
			outputs.write("10aaa",keyOut,valueOut,"o1");
			outputs.write("10cc",keyOut,valueOut,"o2");
			outputs.write("101vb",keyOut,valueOut,"o3");
			context.write(keyOut,valueOut);
		}

		/**
		 * Called once at the end of the task.
		 *
		 * @param context
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			outputs.close();
		}
	}

	private static class SortWordJobGroupComparator extends WritableComparator {

		public SortWordJobGroupComparator() {
			super(MyWritable.class,true);
		}

		@Override
		public int compare(Object a, Object b) {
			MyWritable a1 = (MyWritable) a;
			MyWritable a2 = (MyWritable) b;
			if(a1.getValue().hashCode() == a2.hashCode()){
				if(a1.getSum() > a2.getSum()){
                    return 1;
				}
				if(a1.getSum() < a2.getSum()){
					return -1;
				}
			}
			return -super.compare(a, b);
		}
	}
}
