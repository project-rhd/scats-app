package scats.utils;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * @author Yikai Gong
 */

public class MethodTimer {
    public static <T> T print(Callable<T> task) {
        T call = null;
        try {
            long startTime = System.currentTimeMillis();
            call = task.call();
            TimeUnit time = TimeUnit.MILLISECONDS;
            long duration = System.currentTimeMillis() - startTime;
            System.out.println("Task finished in " +
                    time.toDays(duration) + "D " +
                    time.toHours(duration) + "H " +
                    time.toMinutes(duration) + "M " +
                    time.toSeconds(duration) + "S.");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return call;
    }
}

