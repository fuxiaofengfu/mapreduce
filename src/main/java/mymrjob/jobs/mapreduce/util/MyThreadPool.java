package mymrjob.jobs.mapreduce.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class MyThreadPool {

	private static ExecutorService threadPoolExecutor = Executors.newFixedThreadPool(5);

	public static void excute(Runnable runnable){
		threadPoolExecutor.execute(runnable);
	}

    private static class MyThread implements ThreadFactory{
	    @Override
	    public Thread newThread(Runnable r) {
		    return new Thread(r);
	    }
    }
}
