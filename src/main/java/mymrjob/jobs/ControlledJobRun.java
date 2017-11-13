package mymrjob.jobs;

import mymrjob.jobs.mapreduce.util.MyThreadPool;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class ControlledJobRun {

	public static void main(String[] args) throws Exception {

		WordCountJob wordCount = new WordCountJob();
		Job wordCountJob = wordCount.getJob(args);
		JobControl jobControl = new JobControl(wordCountJob.getJobName());
		ControlledJob controlledJob = new ControlledJob(wordCountJob.getConfiguration());
		jobControl.addJob(controlledJob);

		MyThreadPool.excute(new Runnable() {
			@Override
			public void run() {

				MyThreadPool.threadPoolStop();
				while(!jobControl.allFinished()){
					try {
						Thread.sleep(TimeUnit.MILLISECONDS.toMillis(1000));
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				System.out.println("jobControl execute finished .......");
				List<ControlledJob> failedJobList = jobControl.getFailedJobList();
				for(ControlledJob v : failedJobList){
					System.out.println("失败的任务>>:jobFailedName:"+v.getJobName()
							+";jobFailedId="+v.getJobID());
				}
				jobControl.stop();
				System.out.println("jobControl.stop ........");
			}
		});
		jobControl.run();
	}
}
