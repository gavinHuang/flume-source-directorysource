package org.apache.flume.source;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.DecodeErrorPolicy;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Executors;

/**
 * Created by Gavin.Huang on 2016/4/13.
 */
public class FileEventReaderTest {


    private File tmpDir;
    private File tmpTrackDir;

    @Before
    public void setUp(){
        tmpDir = Files.createTempDir();
        tmpTrackDir = Files.createTempDir();
    }

    @After
    public void tearDown() {
        for (File f : tmpDir.listFiles()) {
            f.delete();
        }
        tmpDir.delete();

        for (File f : tmpTrackDir.listFiles()) {
            f.delete();
        }
        tmpTrackDir.delete();
    }

    @Test
    public void testReadEvents() throws IOException, SQLException, ClassNotFoundException {
        File.createTempFile("test", ".tmp",tmpDir);
        FileEventReader fileEventReader = new FileEventReader.Builder()
                .watchDirectory(tmpDir)
                .startsFrom(DirectorySource.StartFrom.BEGINNING)
                .trackerDirPath(tmpTrackDir.getAbsolutePath())
                .ignorePattern("^$")
                .annotateFileName(true)
                .fileNameHeader("fileName")
                .annotateBaseName(true)
                .baseNameHeader("basename")
                .deserializerType("LINE")
                .deserializerContext(new Context())
                .inputCharset("UTF-8")
                .decodeErrorPolicy(DecodeErrorPolicy.FAIL)
                .executor(Executors.newScheduledThreadPool(5))
                .build();
        fileEventReader.init();

        //create two files, 10 lines for each one
        File testFile = File.createTempFile("test-", ".log", tmpDir);
        for(int i=0;i<10;i++){
            Files.append("testing\n", testFile, Charsets.UTF_8);
        }
        testFile = File.createTempFile("test2-", ".log", tmpDir);
        for(int i=0;i<10;i++){
            Files.append("testing2\n", testFile, Charsets.UTF_8);
        }

        List<Event> eventList = fileEventReader.readEvents(100);
        Assert.assertEquals(10, eventList.size());
        eventList = fileEventReader.readEvents(100);
        Assert.assertEquals(10, eventList.size());
        fileEventReader.commit();

        Assert.assertTrue(eventList.get(0).getHeaders().containsKey("basename"));
    }

}
