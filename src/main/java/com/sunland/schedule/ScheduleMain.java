package com.sunland.schedule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.sunland.schedule.utils.LoggerName.SCHEDULE_LOGGER_NAME;

public class ScheduleMain {
    private static Logger LOGGER = LoggerFactory.getLogger(SCHEDULE_LOGGER_NAME);

    public static void main(String[] args) {
        Thread.setDefaultUncaughtExceptionHandler((thread, exception) ->
                LOGGER.error("UncaughtException in Thread " + thread.toString(), exception));

        if (args.length < 1) {
            LOGGER.error("params error!");
            return;
        }

        System.out.println("args[0] is " + args[0]);
        ScheduleStartup startup = new ScheduleStartup(args[0]);
        try {
            startup.start();
        } catch (Exception e) {
            LOGGER.error("error while start chronos, err:{}", e.getMessage(), e);
            startup.stop();
        }
    }
}
