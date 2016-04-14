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

import org.apache.flume.serialization.PositionTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;


public class SqllitePositionTracker implements PositionTracker {

    private static final Logger logger = LoggerFactory.getLogger(SqllitePositionTracker.class);

    private SqlliteDB sinceDB;
    private String filePath;

    public SqllitePositionTracker(SqlliteDB sinceDB, String filePath){
        this.sinceDB = sinceDB;
        this.filePath = filePath;
    }

    @Override
    public void storePosition(long position) throws IOException {
        try {
            sinceDB.updateLineOffset(filePath, position);
        } catch (SQLException e) {
            logger.error("failed to update offset",e);
            throw new IOException(e);
        }
    }

    @Override
    public long getPosition() {
        try {
            return sinceDB.getLineOffset(filePath);
        } catch (SQLException e) {
            logger.error("failed to get offset",e);
        }
        return 0;
    }

    @Override
    public String getTarget() {
        return filePath;
    }

    @Override
    public void close() throws IOException {

    }
}
