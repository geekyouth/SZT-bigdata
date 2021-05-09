package cn.java666.sztcommon.demo;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * @author Geek
 * @date 2021-05-08 22:01:08
 * 
 * TODO queue demo
 */
public class ThreadQueue {
  
  public static void main(String[] args) {
    
    ArrayBlockingQueue<Integer> queue = new ArrayBlockingQueue<>(100);
    new Thread2(queue).start();
    
    new Thread1(queue).start();
    
    try {
      Thread.sleep(60 * 60 * 1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
  
  static class Thread1 extends Thread {
    private ArrayBlockingQueue<Integer> queue;
    
    public Thread1(ArrayBlockingQueue<Integer> queue) {
      this.queue = queue;
    }
    
    @Override
    public void run() {
      for (int i = 0; i < 10000; i++) {
        try {
          queue.put(i);
          System.out.println(Thread.currentThread().getId() + ": put --- " + i);
          Thread.sleep(2 * 1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }
  
  static class Thread2 extends Thread {
    private ArrayBlockingQueue<Integer> queue;
    
    public Thread2(ArrayBlockingQueue<Integer> queue) {
      this.queue = queue;
    }
    
    @Override
    public void run() {
      for (int i = 0; i < 10000; i++) {
        try {
          int take = queue.take();
          System.out.println(Thread.currentThread().getId() + ": take --- " + take);
          // Thread.sleep(2 * 1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
