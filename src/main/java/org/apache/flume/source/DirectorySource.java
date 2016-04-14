package org.apache.flume.source;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.flume.*;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.serialization.DecodeErrorPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flume.source.DirectorySourceConfigurationConstants.*;
import static org.apache.flume.source.DirectorySourceConfigurationConstants.DEFAULT_MAX_BACKOFF;

public class DirectorySource extends AbstractSource implements Configurable, EventDrivenSource {

    private static final Logger logger = LoggerFactory.getLogger(DirectorySource.class);

    private String watchDirectory;
    private StartFrom startFrom;
    private String trackerDirPath;
    private String ignorePattern;

    private boolean fileHeader;
    private String fileHeaderKey;
    private boolean basenameHeader;
    private String basenameHeaderKey;
    private int batchSize;
    private String deserializerType;
    private Context deserializerContext;
    private String inputCharset;
    private DecodeErrorPolicy decodeErrorPolicy;
    private volatile boolean hasFatalError = false;

    private SourceCounter sourceCounter;
    private FileEventReader reader;
    private ScheduledExecutorService executor;
    private boolean backoff = true;
    private boolean hitChannelException = false;
    private int maxBackoff;

    @Override
    public synchronized void start() {
        if (executor != null && executor.isTerminated()) executor.shutdown();
        executor = Executors.newScheduledThreadPool(5);
        File directory = new File(watchDirectory);

        logger.info("DirectorySource source starting with directory: {}",
                watchDirectory);
        try {
            if (reader != null) reader.close();
            reader = new FileEventReader.Builder()
                    .watchDirectory(directory)
                    .startsFrom(startFrom)
                    .trackerDirPath(trackerDirPath)
                    .ignorePattern(ignorePattern)
                    .annotateFileName(fileHeader)
                    .fileNameHeader(fileHeaderKey)
                    .annotateBaseName(basenameHeader)
                    .baseNameHeader(basenameHeaderKey)
                    .deserializerType(deserializerType)
                    .deserializerContext(deserializerContext)
                    .inputCharset(inputCharset)
                    .decodeErrorPolicy(decodeErrorPolicy)
                    .executor(executor)
                    .build();

        } catch (Exception ex) {
            throw new FlumeException("Error instantiating Directory source",
                    ex);
        }

        Runnable runner = new DirectoryRunnable(reader, sourceCounter);
        executor.submit(runner);

        super.start();
        logger.debug("DirectorySource source started");
        sourceCounter.start();

        //init after runner starts
        try {
            reader.init();
        } catch (Exception e) {
            logger.error("failed to init reader",e);
            throw new FlumeException("Error instantiating Directory source",
                    e);
        }
    }

    @Override
    public synchronized void stop() {
        executor.shutdown();
        try {
            executor.awaitTermination(2L, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            logger.info("Interrupted while awaiting termination", ex);
        }
        executor.shutdownNow();

        super.stop();
        sourceCounter.stop();
        logger.info("Directory source {} stopped. Metrics: {}", getName(),
                sourceCounter);

        if (reader != null) try {
            reader.close();
            reader = null;
        } catch (IOException e) {
            logger.info("close reader:", e);
        }
    }

    @Override
    public String toString() {
        return "Directory source " + getName() +
                ": { watching: " + watchDirectory + " }";
    }

    @Override
    public synchronized void configure(Context context) {
        watchDirectory = context.getString(DIRECTORY);
        Preconditions.checkState(watchDirectory != null, "Configuration must specify a directory");
        ignorePattern = context.getString(IGNORE_PAT, DEFAULT_IGNORE_PAT);
        trackerDirPath = context.getString(TRACKER_DIR, DEFAULT_TRACKER_DIR);
        startFrom = StartFrom.valueOf(context.getString("startFrom", "BEGINNING").toUpperCase(Locale.ENGLISH));

        fileHeader = context.getBoolean(FILENAME_HEADER, DEFAULT_FILE_HEADER);
        fileHeaderKey = context.getString(FILENAME_HEADER_KEY, DEFAULT_FILENAME_HEADER_KEY);
        basenameHeader = context.getBoolean(BASENAME_HEADER, DEFAULT_BASENAME_HEADER);
        basenameHeaderKey = context.getString(BASENAME_HEADER_KEY, DEFAULT_BASENAME_HEADER_KEY);
        batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
        inputCharset = context.getString(INPUT_CHARSET, DEFAULT_INPUT_CHARSET);
        decodeErrorPolicy = DecodeErrorPolicy.valueOf(
                context.getString(DECODE_ERROR_POLICY, DEFAULT_DECODE_ERROR_POLICY)
                        .toUpperCase(Locale.ENGLISH));
        deserializerType = context.getString(DESERIALIZER, DEFAULT_DESERIALIZER);
        deserializerContext = new Context(context.getSubProperties(DESERIALIZER +"."));

        maxBackoff = context.getInteger(MAX_BACKOFF, DEFAULT_MAX_BACKOFF);
        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }
    }

    @VisibleForTesting
    protected boolean hasFatalError() {
        return hasFatalError;
    }



    /**
     * The class always backs off, this exists only so that we can test without
     * taking a really long time.
     * @param backoff - whether the source should backoff if the channel is full
     */
    @VisibleForTesting
    protected void setBackOff(boolean backoff) {
        this.backoff = backoff;
    }

    @VisibleForTesting
    protected boolean hitChannelException() {
        return hitChannelException;
    }

    @VisibleForTesting
    protected SourceCounter getSourceCounter() {
        return sourceCounter;
    }

    private class DirectoryRunnable implements Runnable {
        private FileEventReader reader;
        private SourceCounter sourceCounter;

        public DirectoryRunnable(FileEventReader reader,
                                 SourceCounter sourceCounter) {
            this.reader = reader;
            this.sourceCounter = sourceCounter;
        }

        @Override
        public void run() {
            int backoffInterval = 250;
            try {
                while (!Thread.interrupted()) {
                    List<Event> events = reader.readEvents(batchSize);
                    if (events.isEmpty()) {
                        break;
                    }
                    sourceCounter.addToEventReceivedCount(events.size());
                    sourceCounter.incrementAppendBatchReceivedCount();

                    try {
                        getChannelProcessor().processEventBatch(events);
                        reader.commit();
                    } catch (ChannelException ex) {
                        logger.warn("The channel is full, and cannot write data now. The " +
                                "source will try again after " + String.valueOf(backoffInterval) +
                                " milliseconds");
                        hitChannelException = true;
                        if (backoff) {
                            TimeUnit.MILLISECONDS.sleep(backoffInterval);
                            backoffInterval = backoffInterval << 1;
                            backoffInterval = backoffInterval >= maxBackoff ? maxBackoff :
                                    backoffInterval;
                        }
                        continue;
                    }
                    backoffInterval = 250;
                    sourceCounter.addToEventAcceptedCount(events.size());
                    sourceCounter.incrementAppendBatchAcceptedCount();
                }
            } catch (Throwable t) {
                logger.error("FATAL: " + DirectorySource.this.toString() + ": " +
                        "Uncaught exception in DirectorySource thread. " +
                        "Restart or reconfigure Flume to continue processing.", t);
                hasFatalError = true;
                Throwables.propagate(t);
            }
        }
    }


    @InterfaceAudience.Private
    @InterfaceStability.Unstable
    public static enum StartFrom {
        BEGINNING,
        LAST
    }
}
