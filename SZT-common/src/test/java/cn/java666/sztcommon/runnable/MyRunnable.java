package cn.java666.sztcommon.runnable;

/**
 * @author Geek
 * @date 2021-05-08 21:33:16
 */

public class MyRunnable implements Runnable {

    @Override
    public void run() {
        for (int i = 1; i <= 100; i++) {
            System.out.println(Thread.currentThread().getId() + ": --- " + i);
        }
    }

}
