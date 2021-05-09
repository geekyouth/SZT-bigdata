package cn.java666.sztcommon.thread;

/**
 * @author Geek
 * @date 2021-05-08 21:49:47
 */
public class MyThread extends Thread {
  @Override
  public void run() {
    for (int i = 1; i <= 1000; i++) {
      System.out.println(Thread.currentThread().getId() + ": --- " + i);
    }
  }
}
