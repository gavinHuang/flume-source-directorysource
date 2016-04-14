package org.apache.flume.source;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

/**
 * Created by Gavin.Huang on 2016/4/13.
 */
public class FileLoaderTest {

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
    public void testLoadDirectFile() throws InterruptedException, IOException {
        //prepare
        for(int i = 0; i < 3; i++){
            File testFile = File.createTempFile("fileName"+i, ".tmp",tmpDir);
            Files.write("for test\n", testFile, Charsets.UTF_8);
        }

        //checking
        FileLoader fileLoader = new FileLoader();
        BlockingQueue<File> fileQueue = fileLoader.loadFile(tmpDir,FileLoader.LoadOption.FILE, new FilePathValidator(Pattern.compile("^$")));
        Assert.assertEquals(3, fileQueue.size());
    }

    @Test
    public void testLoadFilesInSubDir() throws InterruptedException, IOException {
        //prepare
        for(int i = 0; i < 3; i++){
            File testFile = File.createTempFile("fileName"+i, ".tmp",tmpDir);
            Files.write("for test\n", testFile, Charsets.UTF_8);
        }
        File subDir = new File(tmpDir,"sub");
        subDir.mkdir();
        for(int i = 0; i < 3; i++){
            File testFile = File.createTempFile("subfileName"+i, ".tmp",subDir);
            Files.write("for sub dir test\n", testFile, Charsets.UTF_8);
        }

        //checking
        FileLoader fileLoader = new FileLoader();
        BlockingQueue<File> fileQueue = fileLoader.loadFile(tmpDir,FileLoader.LoadOption.FILE, new FilePathValidator(Pattern.compile("^$")));
        Assert.assertEquals(6, fileQueue.size());
    }

    @Test
    public void testLoadFilesWithIgnore() throws IOException, InterruptedException {
        //prepare
        File.createTempFile("fileName", ".tmp",tmpDir);
        File.createTempFile("fileName", ".sh",tmpDir);
        File.createTempFile("fileName", ".log",tmpDir);

        FileLoader fileLoader = new FileLoader();
        BlockingQueue<File> fileQueue = fileLoader.loadFile(tmpDir,FileLoader.LoadOption.FILE, new FilePathValidator(Pattern.compile(".+\\.tmp")));
        Assert.assertEquals(2, fileQueue.size());
    }

    @Test
    public void testLoadDirectory() throws IOException, InterruptedException {
        //prepare
        new File(tmpDir,"sub").mkdir();
        new File(tmpDir,"sub1").mkdir();

        FileLoader fileLoader = new FileLoader();
        BlockingQueue<File> fileQueue = fileLoader.loadFile(tmpDir,FileLoader.LoadOption.DIRECTORY, new FilePathValidator(Pattern.compile("^$")));
        Assert.assertEquals(3, fileQueue.size());
    }

}
