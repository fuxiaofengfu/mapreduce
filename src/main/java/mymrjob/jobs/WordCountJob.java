package mymrjob.jobs;

import mymrjob.jobs.mapreduce.AbstractMRJob;
import mymrjob.jobs.mapreduce.MyJobConf;
import mymrjob.jobs.mapreduce.MyWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountJob extends AbstractMRJob {

	@Override
	public Job getJob(String[] args) throws Exception {
		MyJobConf myJobConf = new MyJobConf("wordCountJob",WordCountJob.class,WordCountReducer.class,WordCountMapper.class);
		myJobConf.setReducerOutKey(MyWritable.class);
		myJobConf.setReducerOutValue(MyWritable.class);
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
		Path inputpath = new Path(args[0]);
		Path outputpath = new Path(args[1]);
		FileInputFormat.addInputPath(job,inputpath);
		FileOutputFormat.setOutputPath(job,outputpath);
	}

	private static class WordCountMapper extends Mapper<LongWritable,Text,MyWritable,MyWritable>{

		MyWritable keyOut = new MyWritable();
		MyWritable valueOut = new MyWritable(1);
	    /**
	     * Called once at the beginning of the task.
	     *
	     * @param context
	     */
	    @Override
	    protected void setup(Context context) throws IOException, InterruptedException {
		    logger.info("\nmapStart ...............................");
		    super.setup(context);
	    }

	    /**
	     * Called once for each key/value pair in the input split. Most applications
	     * should override this, but the default is the identity function.
	     *
	     * @param key
	     * @param value
	     * @param context
	     */
	    @Override
	    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		    //logger.info("\nmap >>>>> key....{},value.....{}",key,value);
		    String[] arr = value.toString().split(" ");
		    for(String str : arr){
			    keyOut.setValue(str);
			    context.write(keyOut,valueOut);
		    }
	    }

	    /**
	     * Called once at the end of the task.
	     *
	     * @param context
	     */
	    @Override
	    protected void cleanup(Context context) throws IOException, InterruptedException {
		    super.cleanup(context);
		    logger.info("\nmapFinish ...............................");
	    }
    }

    private static class WordCountReducer extends Reducer<MyWritable,MyWritable,Text,LongWritable>{

	    LongWritable valueOut = new LongWritable(0);
	    Text keyOut = new Text();

	    /**
	     * Called once at the start of the task.
	     * @param context
	     */
	    @Override
	    protected void setup(Context context) throws IOException, InterruptedException {
	    	logger.info("\nreducer start ...............................");
		    super.setup(context);
	    }

	    /**
	     * This method is called once for each key. Most applications will define
	     * their reduce class by overriding this method. The default implementation
	     * is an identity function.
	     *
	     * @param key
	     * @param values
	     * @param context
	     */
	    @Override
	    protected void reduce(MyWritable key, Iterable<MyWritable> values, Context context) throws IOException, InterruptedException {

            long sum = 0;
		    StringBuilder stringBuilder = new StringBuilder();
		    for(MyWritable v : values){
			    sum += v.getSum();
			    stringBuilder.append(String.valueOf(v.getSum()));
		    }
		    context.getCounter("myCount","myTotalWords").increment(sum);
		    /*logger.info("\nreduce key >>>{};;value>>>>{}",key,stringBuilder.toString());*/
		    valueOut.set(sum);
		    keyOut.set(key.getValue());
		    context.write(keyOut,valueOut);
	    }

	    /**
	     * Called once at the end of the task.
	     *
	     * @param context
	     */
	    @Override
	    protected void cleanup(Context context) throws IOException, InterruptedException {
		    super.cleanup(context);
		    logger.info("\nreducer finished ...............................");
	    }
    }
}
