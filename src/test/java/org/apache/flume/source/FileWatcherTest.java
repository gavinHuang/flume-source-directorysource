package org.apache.flume.source;

import com.google.common.io.Files;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

/**
 * Created by Gavin.Huang on 2016/4/13.
 */
public class FileWatcherTest {


    private File tmpDir;

    @Before
    public void setUp(){
        tmpDir = Files.createTempDir();
    }

    @After
    public void tearDown() {
        for (File f : tmpDir.listFiles()) {
            f.delete();
        }
        tmpDir.delete();
    }

    @Test
    public void testWatchNewFile() throws IOException {
        BlockingQueue<File> files = new LinkedBlockingQueue<File>(100);
        FileWatcher fileWatcher = new FileWatcher(tmpDir,new FilePathValidator(Pattern.compile("^$")), files);
        Thread watchingThread = new Thread(fileWatcher);
        watchingThread.start();

        //create new file
        File.createTempFile("test", ".tmp",tmpDir);
        File.createTempFile("test", ".sh",tmpDir);
        File.createTempFile("test", ".log",tmpDir);
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Assert.assertEquals(3, files.size());
    }

    @Test
    public void testWatchNewDirectory() throws IOException {
        BlockingQueue<File> files = new LinkedBlockingQueue<File>(100);
        FileWatcher fileWatcher = new FileWatcher(tmpDir,new FilePathValidator(Pattern.compile("^$")), files);
        Thread watchingThread = new Thread(fileWatcher);
        watchingThread.start();

        //create new file in sub dir
        File file = new File(tmpDir,"sub1");
        file.mkdir();
        File.createTempFile("fileName", ".tmp",file);

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Assert.assertEquals(1, files.size());
    }
}
