package mymrjob.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.Iterator;

public class SortWordJobTest extends Configured implements Tool{

	private static String test_a="543";

	private static final String SPLIT_REGEX = "===";

	/**
	 * Execute the command with the given arguments.
	 *
	 * @param args command specific arguments.
	 * @return exit code.
	 * @throws Exception
	 */
	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Job job = Job.getInstance(conf,"sortWord");

		//conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
		//job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setSortComparatorClass(MySort.class);
		job.setGroupingComparatorClass(MySort2.class);
		job.setPartitionerClass(MyPartitioner.class);
		job.setCombinerClass(MyCombiner.class);
		//job.addCacheFile();
		job.setMapperClass(KeyValueMyMapper.class);
		//job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		//KeyValueTextInputFormat.setInputDirRecursive(job,true);
		//KeyValueTextInputFormat.setInputPaths(job,args[0]);
		FileInputFormat.setInputDirRecursive(job,true);
		FileInputFormat.setInputPaths(job,args[0]);

		Path outPutPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job,outPutPath);

		FileSystem fileSystem = FileSystem.get(conf);
		if(fileSystem.exists(outPutPath)){
			fileSystem.delete(outPutPath,true);
		}

		int result =  job.waitForCompletion(true) ? 0 : 1;
		System.out.println("***************"+test_a);
		return result;
		//return 0;
	}

	private static class MyMapper extends Mapper<LongWritable,Text,Text,LongWritable>{

		/**
		 * Called once at the beginning of the task.
		 *
		 * @param context
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			System.out.println("map start  ...........");
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] arr = value.toString().split("\t");
			System.out.println(key+";value="+value.toString());
			context.write(new Text(arr[0]+SPLIT_REGEX+arr[1]),new LongWritable(Long.parseLong(arr[1])));
		}
	}


	private static class KeyValueMyMapper extends Mapper<Text,Text,Text,LongWritable>{

		/**
		 * Called once at the beginning of the task.
		 *
		 * @param context
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			test_a="retyu";
			System.out.println("map start  ...........");
		}

		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String[] arr = value.toString().split("\t");
			System.out.println(key+";value="+value.toString());
			context.write(new Text(arr[0]+SPLIT_REGEX+arr[1]),new LongWritable(Long.parseLong(arr[1])));
		}
	}

	private static class MySort extends WritableComparator{

		public MySort() {
			super(Text.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			Text text = (Text)a;
			Text text1 = (Text)b;

			String[] key1 = text.toString().split(SPLIT_REGEX);
			String[] key2 = text1.toString().split(SPLIT_REGEX);
			if(key1[0].equals(key2[0])){
				return Integer.compare(Integer.parseInt(key1[1]),Integer.parseInt(key2[1]));
			}
			text.set(key1[0]);
			text1.set(key2[0]);
			int result = text.compareTo(text1);
			System.out.println("sort.....compare="+result+".....;text="+text.toString()+";text_1="+text1.toString());
			return result;
			//return 0;
		}
	}

	private static class MySort2 extends WritableComparator{
		public MySort2() {
			super(Text.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {

			Text text = (Text)a;
			Text text1 = (Text)b;
			//text.set(text.toString().split("==")[0]);
			//text1.set(text1.toString().split("==")[0]);
			System.out.println("group sort..........");
			return text.compareTo(text1);
			//return 0;
		}
	}
	private static class MyReducer extends Reducer<Text,LongWritable,Text,LongWritable>{

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			//super.reduce(key, values, context);
			//context.
			Iterator<LongWritable> iterator = values.iterator();
			StringBuilder builder = new StringBuilder("reduce key=").append(key.toString());
			builder.append(";value=");
			while(iterator.hasNext()){
				LongWritable value = iterator.next();
				builder.append(";").append(value.toString());
				//System.out.println("reduce ...key="+key.toString()+";.value="+value.toString());
				key.set(key.toString().split(SPLIT_REGEX)[0]);
				context.write(key,value);
			}
			System.out.println(builder.toString());
		}
	}

	private static class MyPartitioner extends Partitioner<Text,LongWritable>{

		@Override
		public int getPartition(Text text, LongWritable longWritable, int numPartitions) {
			//int partion = text.hashCode()%numPartitions;
			//System.out.println("MyPartitioner ...............key="+text.toString()+";value="+longWritable.toString()+";keyHash="+text.hashCode()+";partion="+partion);
			//return partion;
			if(text.toString().split(SPLIT_REGEX)[0].contains("2")){
				return 1;
			}

			return 0;
		}
	}

	private static class MyCombiner extends Reducer<Text,LongWritable,Text,LongWritable>{

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			//System.out.printf("my combiner ...........");
			Iterator<LongWritable> iterator = values.iterator();
			while(iterator.hasNext()){
				LongWritable value = iterator.next();
				//key.set(key.toString().split(SPLIT_REGEX)[0]);
				//System.out.println("reduce ...key="+key.toString()+";.value="+value.toString());
				context.write(key,value);
			}
		}
	}
}
