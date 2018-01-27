package exercise.deadlock;


import org.jboss.netty.util.internal.NonReentrantLock;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MyDeadLock {

	Lock lock = new NonReentrantLock();


	public void m1(){
		//lock = new ReentrantLock();
		new Thread(){
			@Override
			public void run() {
				System.out.println("thread 1---");
				lock.lock();
				//lock.unlock();
			}
		}.start();

		new Thread(){
			@Override
			public void run() {
				System.out.println("thread 2---");
				lock.lock();
				//lock.unlock();
			}
		}.start();

	}

	public void m2(){
		lock.lock();
		System.out.println("m2...........");
		lock.unlock();
	}


	public static void main(String[] args) {
		MyDeadLock myDeadLock = new MyDeadLock();
		myDeadLock.m1();
	}
}
