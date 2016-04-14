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

import java.sql.*;
import java.text.SimpleDateFormat;


public class SqlliteDB {

    private static final Logger logger = LoggerFactory.getLogger(SqlliteDB.class);

    private String dbPath;
    private Connection connection = null;
    private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public SqlliteDB(String dbPath){
        this.dbPath = dbPath;
    }

    public void connect() throws SQLException, ClassNotFoundException {
        if (connection == null || connection.isClosed()){
            connection = DriverManager.getConnection("jdbc:sqlite:"+dbPath+"/.since.db");
            initTable();
        }
    }

    public void close()  {
        try {
            if (connection != null && !connection.isClosed()) connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    void initTable() throws SQLException {
        Statement statement = connection.createStatement();
        statement.executeUpdate("create table if not exists history (file string, position long, time string)");
    }

    public long getLineOffset(String filePath) throws SQLException {
        long lineNumber = 0;
        Statement statement = connection.createStatement();
        statement.setQueryTimeout(30);  // set timeout to 30 sec.
        ResultSet rs = statement.executeQuery("select position from history where file='"+filePath+"'");
        if(rs.next())
        {
            lineNumber = rs.getLong(1);
        }
        return lineNumber;
    }

    public void updateLineOffset(String filePath, long line)throws SQLException {
        Statement statement = connection.createStatement();
        String time = simpleDateFormat.format(new java.util.Date());
        ResultSet rs = statement.executeQuery("select count(*) from history where file='" + filePath + "'");
        if(rs.next() && rs.getInt(1) > 0)
        {
            statement.executeUpdate(String.format("UPDATE history SET position = %d,time='%s' WHERE file='%s'",line, time,filePath));
        }else{
            statement.executeUpdate(String.format("INSERT INTO history (file,position,time) VALUES ('%s',%d,'%s')",filePath,line,time));
        }
    }
}
