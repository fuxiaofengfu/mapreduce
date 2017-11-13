package mymrjob.jobs.mapreduce.util;

import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class JobControlMonitor {

	public static void listenJobControl(JobControl jobControl){
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
	}
}
