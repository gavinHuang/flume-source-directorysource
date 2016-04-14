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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.BlockingQueue;


import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

public class FileWatcher implements Runnable{

    private static final Logger logger = LoggerFactory.getLogger(FileWatcher.class);

    private BlockingQueue<File> pendingFileQueue;
    private File watchDirectory;
    private WatchService watcher;
    private FilePathValidator filePathValidator;


    public FileWatcher(File watchDirectory, FilePathValidator filePathValidator,
                       BlockingQueue<File> pendingFileQueue) throws IOException {
        this.watchDirectory = watchDirectory;
        this.pendingFileQueue = pendingFileQueue;
        this.filePathValidator=filePathValidator;
        watcher = FileSystems.getDefault().newWatchService();
        watchDirRecursively(watchDirectory, watcher);
    }

    public void watchDirRecursively(File watchDirectory, WatchService watcher) throws IOException {
        if (!watchDirectory.isDirectory() ||
                !filePathValidator.validateFile(watchDirectory)) {
            return;
        }

        FileLoader fileLoader = new FileLoader();
        BlockingQueue<File> files = fileLoader.loadFile(watchDirectory,
                FileLoader.LoadOption.DIRECTORY, filePathValidator);

        for(File file : files){
            file.toPath().register(watcher, ENTRY_CREATE);
        }
    }


    public void run(){
        while(!Thread.interrupted()){
            try {
                WatchKey watchKey = watcher.take();
                for (WatchEvent<?> event: watchKey.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();
                    if (kind == OVERFLOW) {
                        continue;
                    }
                    WatchEvent<Path> ev = (WatchEvent<Path>) event;
                    Path filename = ev.context();
                    Path parentPath = (Path)watchKey.watchable();
                    Path child = parentPath.resolve(filename);
                    File childFile = child.toFile();
                    if (filePathValidator.validateFile(childFile)){
                        continue;
                    }
                    if (childFile.isDirectory()){
                        try {
                            watchDirRecursively(childFile, watcher);
                        } catch (IOException x) {
                            logger.error("failed to watching dir:", x);
                        }
                    }else{
                        pendingFileQueue.put(childFile);
                    }
                }
                watchKey.reset();
            } catch (InterruptedException ex) {
                logger.error("watching file:", ex);
                return;
            }
        }
    }
}
