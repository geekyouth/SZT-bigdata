/**
 * @author Geek
 * @date 2021-05-08 09:25:33
 */

public class TestRunnable {
  public static void main(String[] args) {
    System.out.println("主线程的ID是： " + Thread.currentThread().getId());
    MyRunnable r1 = new MyRunnable("线程1");
    Thread t1 = new Thread(r1);
    t1.start();
    
    MyRunnable r2 = new MyRunnable("线程2");
    Thread t2 = new Thread(r2);
    /*直接调用run()方法，并不会创建新线程*/
    t2.run();
  }
}

class MyRunnable implements Runnable {
  
  private String name;
  
  public MyRunnable(String name) {
    this.name = name;
  }
  
  @Override
  public void run() {
    System.out.println("名字" + name + "的线程ID是： " + Thread.currentThread().getId());
  }
}
