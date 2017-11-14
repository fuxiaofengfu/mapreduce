package mymrjob;

import mymrjob.jobs.SortWordJob;
import mymrjob.jobs.WordCountJob;
import org.apache.hadoop.util.ToolRunner;

public class StartJob {

	public static void main(String[] args) {
		try {
			System.exit(ToolRunner.run(new SortWordJob(),args));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
