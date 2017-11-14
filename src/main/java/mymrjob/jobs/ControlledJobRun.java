package mymrjob.jobs;

import mymrjob.jobs.mapreduce.util.JobControlMonitor;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ControlledJobRun {

	public static void main(String[] args) throws Exception {
		WordCountJob wordCount = new WordCountJob();
		Job wordCountJob = wordCount.getJob(args);

		Path path = FileOutputFormat.getOutputPath(wordCountJob);
		System.out.println(path.getName());
		JobControl jobControl = new JobControl(wordCountJob.getJobName());
		ControlledJob controlledJob = new ControlledJob(wordCountJob.getConfiguration());
		jobControl.addJob(controlledJob);
		JobControlMonitor.listenJobControl(jobControl);
		jobControl.run();
	}
}
