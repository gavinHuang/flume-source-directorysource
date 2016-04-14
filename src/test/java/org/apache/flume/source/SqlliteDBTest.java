package org.apache.flume.source;

import com.google.common.io.Files;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.SQLException;

/**
 * Created by Gavin.Huang on 2016/4/13.
 */
public class SqlliteDBTest {

    private SqlliteDB sqlliteDB = null;
    private File tmpDir;

    @Before
    public void setUp(){
        tmpDir = Files.createTempDir();
        sqlliteDB = new SqlliteDB(tmpDir.getAbsolutePath());
    }

    @After
    public void tearDown() {
        for (File f : tmpDir.listFiles()) {
            f.delete();
        }
        tmpDir.delete();
    }

    @Test
    public void testGetLineOffset() throws SQLException, ClassNotFoundException {
        sqlliteDB.connect();
        long line = sqlliteDB.getLineOffset("/arbitrary path");
        Assert.assertEquals(line, 0);
        sqlliteDB.close();
    }

    @Test
    public void testUpdateLineOffset() throws SQLException, ClassNotFoundException {
        sqlliteDB.connect();
        sqlliteDB.updateLineOffset("/arbitrary path",100);
        long line = sqlliteDB.getLineOffset("/arbitrary path");
        Assert.assertEquals(line, 100);
        sqlliteDB.close();
    }
}
