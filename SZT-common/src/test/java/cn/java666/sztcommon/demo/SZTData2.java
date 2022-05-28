package cn.java666.sztcommon.demo;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

/**
 * @author Geek
 * @date 2020-04-13 19:11:04
 *
 * TODO 使用多线程+队列来抓取数据
 */
public class SZTData2 {

    @Test
    public void saveDataMultiThread() {
        // 共享队列
        ArrayBlockingQueue<Integer> queue = new ArrayBlockingQueue<>(1000);
        inQueueAsync(queue);
        outQueueSync(queue);

        System.out.println("--------------------------------------------");
    }

    /** 新线程入队 */
    public void inQueueAsync(ArrayBlockingQueue<Integer> queue) {
        Runnable run1 = () -> {
            for (int i = 1; i <= 1337 * 1000; i++) {
                try {
                    boolean offer = queue.offer(i, Long.MAX_VALUE, TimeUnit.SECONDS);
                    if (offer) {
                        System.out.println("Thread_name=" + Thread.currentThread().getName() + ", offer=" + offer + ", i=" + i);
                    } else {
                        System.err.println("Thread_name=" + Thread.currentThread().getName() + ", offer=" + offer + ", i=" + i);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        new Thread(run1).start();
    }

    /** 新线程出队 */
    public void outQueueSync(ArrayBlockingQueue<Integer> queue) {
        Runnable run1 = () -> {
            try {
                while (true) {
                    try {
                        Integer poll = queue.poll(3, TimeUnit.SECONDS);
                        if (poll != null) {
                            System.out.println("Thread_name=" + Thread.currentThread().getName() + ", poll=" + poll);
                        } else {
                            System.err.println("Thread_name=" + Thread.currentThread().getName() + ", poll=" + poll);
                            throw new NullPointerException("队列消费完毕");
                        }
                        Thread.sleep(0);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } catch (NullPointerException e) {
                // e.printStackTrace();
                System.out.println(e.getMessage());
            }
        };

        // new Thread(run1).start();
        new Thread(run1).run();
    }
}
