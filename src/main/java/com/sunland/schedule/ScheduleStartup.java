package com.sunland.schedule;

import com.sunland.schedule.db.CFManager;
import com.sunland.schedule.db.RDB;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sunland.schedule.config.ConfigManager;

import java.util.concurrent.TimeUnit;

import static com.sunland.schedule.utils.LoggerName.SCHEDULE_LOGGER_NAME;

public class ScheduleStartup {

    private static final Logger LOGGER = LoggerFactory.getLogger(SCHEDULE_LOGGER_NAME);
    private String configFilePath = "schedule.yaml";
    private int startCount = 10000;
    private int endCount = 20000;


    ScheduleStartup(final String configFilePath) {
        if (StringUtils.isNotBlank(configFilePath)) {
            this.configFilePath = configFilePath;
        }
    }

    public void start() throws Exception {
        LOGGER.info("start to launch chronos...");
        final long start = System.currentTimeMillis();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    LOGGER.info("start to stop chronos...");
                    final long start = System.currentTimeMillis();
                    ScheduleStartup.this.stop();
                    final long cost = System.currentTimeMillis() - start;
                    LOGGER.info("succ stop chronos, cost:{}ms", cost);
                } catch (Exception e) {
                    LOGGER.error("error while shutdown chronos, err:{}", e.getMessage(), e);
                } finally {
                    /* shutdown log4j2 */
                    LogManager.shutdown();
                }
            }
        });

        /* 注意: 以下初始化顺序有先后次序 */

        /* init config */
        ConfigManager.initConfig(configFilePath);

        /* init rocksdb */
        RDB.init(ConfigManager.getConfig().getDbConfig().getDbPath());

        LOGGER.info("start write data");
        long write_start = System.currentTimeMillis();
        writeData();
        long write_end = System.currentTimeMillis();
        LOGGER.info("start write data over, write data cost {}", write_end - write_start);

        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        LOGGER.info("start write data");
        long read_start = System.currentTimeMillis();
        readData();
        long read_end = System.currentTimeMillis();
        LOGGER.info("start write data over, write data cost {}", read_end - read_start);
    }

    void stop() {

        /* close rocksdb */
        RDB.close();

    }

    public void writeData() {
        WriteBatch wb = new WriteBatch();
        ColumnFamilyHandle cfHandle = CFManager.CFH_DEFAULT;

        long st = System.currentTimeMillis();
        for (int i = startCount; i < endCount; i++) {
            wb.put(cfHandle, ("1324356527-" + i + "-5-5-345-356-234-232").getBytes(), "tasdfasdgasdfestfordb".getBytes());

//            if(i % 30 == 0) {
//                RDB.writeAsync(wb);
//                wb.clear();
//            }
        }
        for (int i = startCount; i < endCount; i++) {
            wb.put(cfHandle, ("1324356525-" + i + "-5-5-345-356-234-232").getBytes(), "tasdfasdgasdfestfordb".getBytes());

//            if(i % 30 == 0) {
//                RDB.writeAsync(wb);
//                wb.clear();
//            }
        }
        for (int i = startCount; i < endCount; i++) {
            wb.put(cfHandle, ("1324356529-" + i + "-5-5-345-356-234-232").getBytes(), "tasdfasdgasdfestfordb".getBytes());

//            if(i % 30 == 0) {
//                RDB.writeAsync(wb);
//                wb.clear();
//            }
        }
        RDB.writeAsync(wb);

    }

    public void readData() {
        ColumnFamilyHandle cfHandle = CFManager.CFH_DEFAULT;

        long start = System.currentTimeMillis();
        RocksIterator it = RDB.newIterator(cfHandle);
        byte[] now = "1324356527".getBytes();
        long count = 0;
        for (it.seek(now); it.isValid(); it.next()) {
//            System.out.println(new String(it.key()) + " " + new String(it.value()));
            count++;
            if (count == endCount - startCount)
                break;
        }
        it.close();
        long end = System.currentTimeMillis();
        System.out.println("cost : " + (end - start) + " count:" + count);
//        RDB.deleteFilesInRange(CFManager.CFH_DEFAULT, "132435653".getBytes(), "1324356529".getBytes());

        count = 0;
        it = RDB.newIterator(cfHandle);
        now = "1324356525".getBytes();
        start = System.currentTimeMillis();
        for (it.seek(now); it.isValid(); it.next()) {
//            System.out.println(new String(it.key()) + " " + new String(it.value()));
            count++;
            if (count == endCount - startCount)
                break;
        }
        it.close();
        end = System.currentTimeMillis();
        System.out.println("cost : " + (end - start) + " count:" + count);
    }


}

