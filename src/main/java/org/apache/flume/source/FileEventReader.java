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

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.client.avro.ReliableEventReader;
import org.apache.flume.serialization.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;

public class FileEventReader implements ReliableEventReader {

    private static final Logger logger = LoggerFactory.getLogger(FileEventReader.class);

    ExecutorService executor;

    //queue to store files that been newly created/discovered and thus need to be ingested.
    private final BlockingQueue<File> pendingFileQueue = new LinkedBlockingQueue<File>();

    //starts from beginning will cause list the
    private DirectorySource.StartFrom startsFrom = DirectorySource.StartFrom.BEGINNING;

    //watcher
    private WatchService watcher;
    private SqlliteDB sinceDB;

    private final File watchDirectory;
    private final File trackDirectory;
    private FileWatcher fileWatcher;

    private final String deserializerType;
    private final Context deserializerContext;
    private final Pattern ignorePattern;
    private final boolean annotateFileName;
    private final boolean annotateBaseName;
    private final String fileNameHeader;
    private final String baseNameHeader;
    private final Charset inputCharset;
    private final DecodeErrorPolicy decodeErrorPolicy;

    private Optional<FileInfo> currentFile = Optional.absent();
    private boolean committed = true;

    private FileEventReader(File watchDirectory, String ignorePattern, String trackerDirPath,
                            boolean annotateFileName, String fileNameHeader,
                            boolean annotateBaseName, String baseNameHeader,
                            String deserializerType, Context deserializerContext,
                            String inputCharset, DecodeErrorPolicy decodeErrorPolicy,
                            ExecutorService executor) throws IOException {
        // Sanity checks
        Preconditions.checkNotNull(watchDirectory);
        Preconditions.checkNotNull(ignorePattern);
        Preconditions.checkNotNull(trackerDirPath);
        Preconditions.checkNotNull(deserializerType);
        Preconditions.checkNotNull(deserializerContext);
        Preconditions.checkNotNull(inputCharset);

        // Verify directory exists and is readable/writable
        Preconditions.checkState(watchDirectory.exists(),
                "Directory does not exist: " + watchDirectory.getAbsolutePath());
        Preconditions.checkState(watchDirectory.isDirectory(),
                "Path is not a directory: " + watchDirectory.getAbsolutePath());

        this.watchDirectory = watchDirectory;
        this.deserializerType = deserializerType;
        this.deserializerContext = deserializerContext;
        this.annotateFileName = annotateFileName;
        this.fileNameHeader = fileNameHeader;
        this.annotateBaseName = annotateBaseName;
        this.baseNameHeader = baseNameHeader;
        this.ignorePattern = Pattern.compile(ignorePattern);
        this.inputCharset = Charset.forName(inputCharset);
        this.decodeErrorPolicy = Preconditions.checkNotNull(decodeErrorPolicy);
        this.executor = executor;

        File trackerDirectory = new File(trackerDirPath);
        // if relative path, treat as relative to watch directory
        if (!trackerDirectory.isAbsolute()) {
            trackerDirectory = new File(watchDirectory, trackerDirPath);
        }
        // ensure that meta directory exists
        if (!trackerDirectory.exists()) {
            if (!trackerDirectory.mkdir()) {
                throw new IOException("Unable to mkdir nonexistent meta directory " +
                        trackerDirectory);
            }
        }
        // ensure that the meta directory is a directory
        if (!trackerDirectory.isDirectory()) {
            throw new IOException("Specified meta directory is not a directory" +
                    trackerDirectory);
        }
        // Do a canary test to make sure we have access to watch directory
        try {
            File canary = File.createTempFile("flume-perm-check-", ".db",
                    trackerDirectory);
            com.google.common.io.Files.write("testing flume file permissions\n", canary, Charsets.UTF_8);
            List<String> lines = com.google.common.io.Files.readLines(canary, Charsets.UTF_8);
            Preconditions.checkState(!lines.isEmpty(), "Empty canary file %s", canary);
            if (!canary.delete()) {
                throw new IOException("Unable to delete canary file " + canary);
            }
            logger.debug("Successfully created and deleted canary file: {}", canary);
        } catch (IOException e) {
            throw new FlumeException("Unable to read and modify files" +
                    " in the tracking directory: " + watchDirectory, e);
        }
        trackDirectory = trackerDirectory;
    }

    public void init() throws IOException, SQLException, ClassNotFoundException {
        if (sinceDB != null ) sinceDB.close();
        sinceDB = new SqlliteDB(this.trackDirectory.getAbsolutePath());
        sinceDB.connect();

        final FilePathValidator filePathValidator = new FilePathValidator(ignorePattern);
        //start feeding with existing files,
        // for files that has been processed, the since db will make sure they not processed twice
        if (startsFrom.equals(DirectorySource.StartFrom.BEGINNING)){
            executor.submit(new Runnable(){
                @Override
                public void run() {
                    FileLoader existingFileLoader = new FileLoader();
                    try {
                        pendingFileQueue.addAll(existingFileLoader.loadFile(watchDirectory,
                                FileLoader.LoadOption.FILE, filePathValidator));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        //start watching directories
        fileWatcher = new FileWatcher(watchDirectory, filePathValidator, pendingFileQueue);
        executor.submit(fileWatcher);
    }

    public Event readEvent() throws IOException {
        List<Event> events = readEvents(1);
        if (!events.isEmpty()) {
            return events.get(0);
        } else {
            return null;
        }
    }

    public List<Event> readEvents(int numEvents) throws IOException {
        if (!committed) {
            logger.info("Last read was never committed - resetting mark position.");
            currentFile.get().getDeserializer().reset();
        } else {
            if (!currentFile.isPresent()) {
                currentFile = getNextFile();
            }
            // Return empty list if no new files
            if (!currentFile.isPresent()) {
                return Collections.emptyList();
            }
        }

        EventDeserializer des = currentFile.get().getDeserializer();
        List<Event> events = des.readEvents(numEvents);

        while (events.isEmpty()) {
            logger.info("Rolling to the next file...");
            retireCurrentFile();
            currentFile = getNextFile();
            if (!currentFile.isPresent()) {
                return Collections.emptyList();
            }
            events = currentFile.get().getDeserializer().readEvents(numEvents);
        }

        if (annotateFileName) {
            String filename = currentFile.get().getFile().getAbsolutePath();
            for (Event event : events) {
                event.getHeaders().put(fileNameHeader, filename);
            }
        }

        if (annotateBaseName) {
            String basename = currentFile.get().getFile().getName();
            for (Event event : events) {
                event.getHeaders().put(baseNameHeader, basename);
            }
        }
        committed = false;
        return events;
    }

    Optional<FileInfo> openFile(File file) {
        try {
            String nextPath = file.getPath();
            PositionTracker tracker = new SqllitePositionTracker(sinceDB, nextPath);
            ResettableInputStream in =
                    new ResettableFileInputStream(file, tracker,
                            ResettableFileInputStream.DEFAULT_BUF_SIZE, inputCharset,
                            decodeErrorPolicy);
            EventDeserializer deserializer = EventDeserializerFactory.getInstance
                    (deserializerType, deserializerContext, in);
            return Optional.of(new FileInfo(file, deserializer));
        } catch (Exception e) {
            logger.error("Exception opening file: " + file, e);
            return Optional.absent();
        }
    }

    @Override
    public void close() throws IOException {
        if (currentFile.isPresent()) {
            currentFile.get().getDeserializer().close();
            currentFile = Optional.absent();
        }
        sinceDB.close();
        sinceDB = null;
    }

    /** Commit the last lines which were read. */
    @Override
    public void commit() throws IOException {
        if (!committed) {
            currentFile.get().getDeserializer().mark();
            committed = true;
        }
    }

    private void retireCurrentFile() throws IOException {
        if (currentFile.isPresent()) {
            currentFile.get().getDeserializer().close();
        }
    }

    private Optional<FileInfo> getNextFile() {
        try {
            File file = pendingFileQueue.take();
            return openFile(file);
        } catch (InterruptedException e) {
            logger.info("stopped when waiting for file");
        }
        return Optional.absent();
    }

    private static class FileInfo {
        private final File file;
        private final long length;
        private final long lastModified;
        private final EventDeserializer deserializer;

        public FileInfo(File file, EventDeserializer deserializer) {
            this.file = file;
            this.length = file.length();
            this.lastModified = file.lastModified();
            this.deserializer = deserializer;
        }

        public long getLength() { return length; }
        public long getLastModified() { return lastModified; }
        public EventDeserializer getDeserializer() { return deserializer; }
        public File getFile() { return file; }
    }


    public static class Builder {
        private File watchDirectory;

        private DirectorySource.StartFrom startsFrom =
                DirectorySource.StartFrom.BEGINNING;
        private String ignorePattern =
                DirectorySourceConfigurationConstants.DEFAULT_IGNORE_PAT;
        private String trackerDirPath =
                DirectorySourceConfigurationConstants.DEFAULT_TRACKER_DIR;
        private Boolean annotateFileName =
                DirectorySourceConfigurationConstants.DEFAULT_FILE_HEADER;
        private String fileNameHeader =
                DirectorySourceConfigurationConstants.DEFAULT_FILENAME_HEADER_KEY;
        private Boolean annotateBaseName =
                DirectorySourceConfigurationConstants.DEFAULT_BASENAME_HEADER;
        private String baseNameHeader =
                DirectorySourceConfigurationConstants.DEFAULT_BASENAME_HEADER_KEY;
        private String deserializerType =
                DirectorySourceConfigurationConstants.DEFAULT_DESERIALIZER;
        private Context deserializerContext = new Context();
        private String inputCharset =
                DirectorySourceConfigurationConstants.DEFAULT_INPUT_CHARSET;
        private DecodeErrorPolicy decodeErrorPolicy = DecodeErrorPolicy.valueOf(
                DirectorySourceConfigurationConstants.DEFAULT_DECODE_ERROR_POLICY
                        .toUpperCase(Locale.ENGLISH));

        ExecutorService executor;

        public Builder watchDirectory(File directory) {
            this.watchDirectory = directory;
            return this;
        }

        public Builder ignorePattern(String ignorePattern) {
            this.ignorePattern = ignorePattern;
            return this;
        }

        public Builder trackerDirPath(String trackerDirPath) {
            this.trackerDirPath = trackerDirPath;
            return this;
        }

        public Builder annotateFileName(Boolean annotateFileName) {
            this.annotateFileName = annotateFileName;
            return this;
        }

        public Builder fileNameHeader(String fileNameHeader) {
            this.fileNameHeader = fileNameHeader;
            return this;
        }

        public Builder annotateBaseName(Boolean annotateBaseName) {
            this.annotateBaseName = annotateBaseName;
            return this;
        }

        public Builder baseNameHeader(String baseNameHeader) {
            this.baseNameHeader = baseNameHeader;
            return this;
        }

        public Builder deserializerType(String deserializerType) {
            this.deserializerType = deserializerType;
            return this;
        }

        public Builder deserializerContext(Context deserializerContext) {
            this.deserializerContext = deserializerContext;
            return this;
        }

        public Builder inputCharset(String inputCharset) {
            this.inputCharset = inputCharset;
            return this;
        }

        public Builder decodeErrorPolicy(DecodeErrorPolicy decodeErrorPolicy) {
            this.decodeErrorPolicy = decodeErrorPolicy;
            return this;
        }

        public Builder startsFrom(DirectorySource.StartFrom startsFrom){
            this.startsFrom =startsFrom;
            return this;
        }

        public Builder executor(ExecutorService executor){
            this.executor = executor;
            return this;
        }

        public FileEventReader build() throws IOException {
            return new FileEventReader(watchDirectory,
                    ignorePattern, trackerDirPath, annotateFileName, fileNameHeader,
                    annotateBaseName, baseNameHeader, deserializerType,
                    deserializerContext,  inputCharset, decodeErrorPolicy,
                    executor);
        }
    }
}
