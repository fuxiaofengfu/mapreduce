package mymrjob.jobs;

import mymrjob.jobs.mapreduce.util.JobControlMonitor;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

public class ControlledJobRun {

	public static void main(String[] args) throws Exception {

		WordCountJob wordCount = new WordCountJob();
		Job wordCountJob = wordCount.getJob(args);
		JobControl jobControl = new JobControl(wordCountJob.getJobName());
		ControlledJob controlledJob = new ControlledJob(wordCountJob.getConfiguration());
		jobControl.addJob(controlledJob);
		JobControlMonitor.listenJobControl(jobControl);
		jobControl.run();
	}
}
