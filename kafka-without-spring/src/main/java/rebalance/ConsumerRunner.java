package rebalance;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerRunner {

    public static final String GROUP_ID = "ReBalance_Consumers_Group";
    private static final ExecutorService SERVICE = Executors.newFixedThreadPool(2);

    public static void main(String[] args) throws InterruptedException {
        for (int i = 0; i < 2; i++) {
            SERVICE.submit(new ConsumerWorker(false));
        }
        Thread.sleep(5000);
        new Thread(new ConsumerWorker(true)).start();
    }
}
