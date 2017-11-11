package mymrjob.jobs.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class AbstractConfTool extends Configured implements Tool {

	protected static Logger logger = LoggerFactory.getLogger(AbstractConfTool.class);

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

		Configuration configuration = getConf();
		Job  job = Job.getInstance(configuration, myJobConf.getJobname());

		job.setJarByClass(myJobConf.getJarByClass());
		job.setMapperClass(myJobConf.getMapper());
        job.setReducerClass(myJobConf.getReducer());

        job.setMapOutputKeyClass(myJobConf.getMapOutKey());
        job.setMapOutputValueClass(myJobConf.getMapOutValue());
        job.setOutputKeyClass(myJobConf.getReducerOutKey());
        job.setOutputValueClass(myJobConf.getReducerOutValue());

		//LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

		if(HandleType.MULTIPLE_INPUT.equals(myJobConf.getHandleType())){
			this.multipleInputPath(args,job);
		}else{
			this.handlePath(args,job);
		}
		return job.waitForCompletion(false) ? 0 : 1;
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

}
