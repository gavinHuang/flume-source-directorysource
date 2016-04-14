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
import org.apache.flume.serialization.DecodeErrorPolicy;


public interface DirectorySourceConfigurationConstants {

    public static final java.lang.String DIRECTORY = "Dir";
    public static final java.lang.String FILENAME_HEADER_KEY = "fileHeaderKey";
    public static final java.lang.String DEFAULT_FILENAME_HEADER_KEY = "file";
    public static final java.lang.String FILENAME_HEADER = "fileHeader";
    public static final boolean DEFAULT_FILE_HEADER = false;
    public static final java.lang.String BASENAME_HEADER_KEY = "basenameHeaderKey";
    public static final java.lang.String DEFAULT_BASENAME_HEADER_KEY = "basename";
    public static final java.lang.String BASENAME_HEADER = "basenameHeader";
    public static final boolean DEFAULT_BASENAME_HEADER = false;
    public static final java.lang.String BATCH_SIZE = "batchSize";
    public static final int DEFAULT_BATCH_SIZE = 100;

    public static final java.lang.String IGNORE_PAT = "ignorePattern";
    public static final java.lang.String DEFAULT_IGNORE_PAT = "^$";
    public static final java.lang.String TRACKER_DIR = "trackerDir";
    public static final java.lang.String DEFAULT_TRACKER_DIR = ".track";
    public static final java.lang.String DESERIALIZER = "deserializer";
    public static final java.lang.String DEFAULT_DESERIALIZER = "LINE";
    public static final java.lang.String INPUT_CHARSET = "inputCharset";
    public static final java.lang.String DEFAULT_INPUT_CHARSET = "UTF-8";
    public static final java.lang.String DECODE_ERROR_POLICY = "decodeErrorPolicy";
    public static final java.lang.String DEFAULT_DECODE_ERROR_POLICY = DecodeErrorPolicy.FAIL.name();
    public static final java.lang.String MAX_BACKOFF = "maxBackoff";
    public static final java.lang.Integer DEFAULT_MAX_BACKOFF = 4000;

}
