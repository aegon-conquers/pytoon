package com.example.scheduledjob;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Instant;

/**
 * Service to handle scheduled job execution with HBase locking mechanism.
 */
@Service
public class ScheduledJobService {

    private static final Logger logger = LoggerFactory.getLogger(ScheduledJobService.class);
    private static final String LOCK_ROW_KEY = "scheduled_job_lock";
    private static final String LOCK_TABLE_NAME = "scheduled_job_lock_table";
    private static final String LOCK_COLUMN_FAMILY = "lock_cf";
    private static final String LOCK_COLUMN = "lock";

    @Autowired
    private Connection hbaseConnection;

    /**
     * Method to be executed every 5 minutes. Acquires a lock in HBase to ensure only one pod runs the job.
     */
    @Scheduled(cron = "0 */5 * * * *")
    public void processJob() {
        logger.info("Attempting to acquire lock and run scheduled job at {}", Instant.now());

        try (Table lockTable = hbaseConnection.getTable(TableName.valueOf(LOCK_TABLE_NAME))) {
            if (acquireLock(lockTable)) {
                try {
                    // Execute the actual job here
                    performScheduledTask();
                } finally {
                    // Release the lock after the job is done
                    releaseLock(lockTable);
                }
            } else {
                logger.info("Lock already acquired by another instance. Skipping this run.");
            }
        } catch (IOException e) {
            logger.error("Error while accessing HBase", e);
        }
    }

    /**
     * Acquires a lock in HBase to ensure only one pod runs the job.
     *
     * @param lockTable The HBase table used for locking.
     * @return true if the lock was successfully acquired, false otherwise.
     * @throws IOException if there is an issue accessing HBase.
     */
    private boolean acquireLock(Table lockTable) throws IOException {
        Get get = new Get(Bytes.toBytes(LOCK_ROW_KEY));
        Result result = lockTable.get(get);
        if (result.isEmpty()) {
            // No lock present, acquire it
            Put put = new Put(Bytes.toBytes(LOCK_ROW_KEY));
            put.addColumn(Bytes.toBytes(LOCK_COLUMN_FAMILY), Bytes.toBytes(LOCK_COLUMN), Bytes.toBytes("locked"));
            lockTable.put(put);
            logger.info("Lock acquired successfully.");
            return true;
        }
        logger.info("Lock already present. Could not acquire lock.");
        return false;
    }

    /**
     * Releases the lock in HBase after the job is completed.
     *
     * @param lockTable The HBase table used for locking.
     * @throws IOException if there is an issue accessing HBase.
     */
    private void releaseLock(Table lockTable) throws IOException {
        Delete delete = new Delete(Bytes.toBytes(LOCK_ROW_KEY));
        lockTable.delete(delete);
        logger.info("Lock released successfully.");
    }

    /**
     * The actual task to be performed by the scheduled job.
     * Implement your business logic here.
     */
    private void performScheduledTask() {
        logger.info("Performing the scheduled task...");
        // Your business logic here
    }
}
