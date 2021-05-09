package cn.java666.sztcommon.thread;

/**
 * @author Geek
 * @date 2021-05-08 21:54:47
 */
public class ThreadDemo {
  public static void main(String[] args) {
    new MyThread().start();
    new MyThread().start();
    try {
      Thread.sleep(1000*1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
