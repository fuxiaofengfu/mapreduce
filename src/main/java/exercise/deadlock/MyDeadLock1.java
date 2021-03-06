package exercise.deadlock;

import java.util.Date;

public class MyDeadLock1 {

	public static String obj1 = "obj1";
	public static String obj2 = "obj2";
	public static void main(String[] args) {
		LockA la = new MyDeadLock1().new LockA();
		new Thread(la).start();
		LockB lb = new MyDeadLock1().new LockB();
		new Thread(lb).start();
	}
	class LockA implements Runnable{
		public void run() {
			try {
				System.out.println(new Date().toString() + " LockA 开始执行");
				while(true){
					synchronized (MyDeadLock1.obj1) {
						System.out.println("Thread LockA得到锁obj1,Thread LockA被锁住");
						Thread.sleep(3000); // 此处等待是给B能锁住机会
						System.out.println("Thread LockA 获取锁obj2中.....");
						synchronized (MyDeadLock1.obj2) {
							System.out.println("Thread LockA得到锁obj2,Thread LockA 被锁住 _______");
							Thread.sleep(60 * 1000); // 为测试，占用了就不放
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	class LockB implements Runnable{
		public void run() {
			try {
				System.out.println(new Date().toString() + " LockB 开始执行");
				while(true){
					synchronized (MyDeadLock1.obj2) {
						System.out.println("Thread LockB得到锁 obj2, Thread LockB 被锁住");
						Thread.sleep(3000); // 此处等待是给A能锁住机会
						System.out.println("Thread LockB 获取锁obj1中.....");
						synchronized (MyDeadLock1.obj1) {
							System.out.println( "Thread LockB 得到锁obj1,Thread LockB 被锁住 ______");
							Thread.sleep(60 * 1000); // 为测试，占用了就不放
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
