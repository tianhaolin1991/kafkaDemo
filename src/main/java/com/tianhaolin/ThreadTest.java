package com.tianhaolin;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadTest {
    public static void main(String[] args) throws InterruptedException {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(2,
                2, 0, TimeUnit.MILLISECONDS,
                new LinkedBlockingDeque<>(2), new ThreadPoolExecutor.AbortPolicy());

        for (int i = 0; i < 10; i++) {
            final int index = i;
            System.out.println(i);
            Thread.sleep(10);
            executor.submit(()->{
                try {
                    Thread.sleep(1000);
                    System.out.println("task:"+index);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(1000,TimeUnit.MILLISECONDS);
    }
}
