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
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;



public class FileLoader{

    private static final Logger logger = LoggerFactory.getLogger(FileLoader.class);

    public enum LoadOption{
        DIRECTORY,
        FILE,
        ALL
    }

    public BlockingQueue<File> loadFile(File root, final LoadOption loadOption,
                         final FilePathValidator filePathValidator) throws IOException {
        final BlockingQueue<File> fileQueue = new LinkedBlockingDeque<File>();
        Files.walkFileTree(root.toPath(), new SimpleFileVisitor<Path>(){
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                    throws IOException
            {
                try {
                    if (loadOption.equals(LoadOption.ALL) || loadOption.equals(LoadOption.DIRECTORY)){
                        if (filePathValidator.validateFile(dir.toFile())){
                            fileQueue.put(dir.toFile());
                        }
                    }
                } catch (InterruptedException e) {
                    logger.error("failed to put file object to queue:",e);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                    throws IOException {
                if (loadOption.equals(LoadOption.ALL) || loadOption.equals(LoadOption.FILE)){
                    try {
                        if (filePathValidator.validateFile(file.toFile())){
                            fileQueue.put(file.toFile());
                        }
                    } catch (InterruptedException e) {
                        logger.error("failed to put file object to queue:",e);
                    }
                }
                return FileVisitResult.CONTINUE;
            }
        });
        return fileQueue;
    }
}
