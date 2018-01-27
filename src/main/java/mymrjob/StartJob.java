package mymrjob;

import mymrjob.jobs.SortWordJobTest;
import org.apache.hadoop.util.ToolRunner;

public class StartJob {

	public static void main(String[] args) {
		try {
			System.exit(ToolRunner.run(new SortWordJobTest(),args));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
