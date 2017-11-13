package mymrjob.jobs.mapreduce.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class MyThreadPool {

	private static ExecutorService threadPoolExecutor = Executors.newSingleThreadExecutor();

	public static void excute(Runnable runnable){
		threadPoolExecutor.execute(runnable);
	}

	public static void threadPoolStop(){
		threadPoolExecutor.shutdown();
		System.out.println("threadPoolExecutor stoping .............");
	}

    private static class MyThread implements ThreadFactory{
	    @Override
	    public Thread newThread(Runnable r) {
		    return new Thread(r);
	    }
    }
}
