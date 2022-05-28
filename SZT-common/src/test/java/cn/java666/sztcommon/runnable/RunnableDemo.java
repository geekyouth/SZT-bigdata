package cn.java666.sztcommon.runnable;

/**
 * @author Geek
 * @date 2021-05-08 21:32:52
 *
 * https://www.jianshu.com/p/94c1c34053d0
 */
public class RunnableDemo {
    public static void main(String[] args) {
        System.out.println(Thread.currentThread().getId());

        new Thread(new MyRunnable()).start();
        // new Thread(new MyRunnable()).start();
        new Thread(new MyRunnable()).start();

        try {
            Thread.sleep(1000 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
