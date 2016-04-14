package org.apache.flume.source;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.flume.*;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleState;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by Gavin.Huang on 2016/4/13.
 */
public class DirectorySourceTest {

    static DirectorySource source;
    static MemoryChannel channel;
    private File tmpDir;
    private File trackDir;

    @Before
    public void setUp() {
        source = new DirectorySource();
        channel = new MemoryChannel();

        Configurables.configure(channel, new Context());

        List<Channel> channels = new ArrayList<Channel>();
        channels.add(channel);

        ChannelSelector rcs = new ReplicatingChannelSelector();
        rcs.setChannels(channels);

        source.setChannelProcessor(new ChannelProcessor(rcs));
        tmpDir = Files.createTempDir();
        trackDir = Files.createTempDir();
    }

    @After
    public void tearDown() {
        for (File f : tmpDir.listFiles()) {
            f.delete();
        }
        tmpDir.delete();
        for (File f : trackDir.listFiles()) {
            f.delete();
        }
        trackDir.delete();
    }

    @Test
    public void testLifecycle() throws IOException, InterruptedException {
        Context context = new Context();
        File f1 = new File(tmpDir.getAbsolutePath() + "/file1");

        Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                        "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
                f1, Charsets.UTF_8);

        context.put(DirectorySourceConfigurationConstants.DIRECTORY,
                tmpDir.getAbsolutePath());
        context.put(DirectorySourceConfigurationConstants.TRACKER_DIR,
                trackDir.getAbsolutePath());

        Configurables.configure(source, context);

        for (int i = 0; i < 10; i++) {
            source.start();
            Assert.assertTrue("Reached start or error", LifecycleController.waitForOneOf(
                            source, LifecycleState.START_OR_ERROR));
            Assert.assertEquals("Server is started", LifecycleState.START,
                    source.getLifecycleState());

            source.stop();
            Assert.assertTrue("Reached stop or error",LifecycleController.waitForOneOf(source, LifecycleState.STOP_OR_ERROR));
            Assert.assertEquals("Server is stopped", LifecycleState.STOP, source.getLifecycleState());
        }
    }

    @Test
    public void testReconfigure() throws InterruptedException, IOException {
        final int NUM_RECONFIGS = 20;
        for (int i = 0; i < NUM_RECONFIGS; i++) {
            Context context = new Context();
            File file = new File(tmpDir.getAbsolutePath() + "/file-" + i);
            Files.write("File " + i, file, Charsets.UTF_8);
            context.put(DirectorySourceConfigurationConstants.DIRECTORY,
                    tmpDir.getAbsolutePath());
            context.put(DirectorySourceConfigurationConstants.TRACKER_DIR,
                    trackDir.getAbsolutePath());
            Configurables.configure(source, context);
            source.start();
            Thread.sleep(TimeUnit.SECONDS.toMillis(1));
            Transaction txn = channel.getTransaction();
            txn.begin();
            try {
                Event event = channel.take();
                String content = new String(event.getBody(), Charsets.UTF_8);
                Assert.assertEquals("File " + i, content);
                txn.commit();
            } catch (Throwable t) {
                txn.rollback();
            } finally {
                txn.close();
            }
            source.stop();
            Assert.assertFalse("Fatal error on iteration " + i, source.hasFatalError());
        }
    }


    @Test
    public void testSourceDoesNotDieOnFullChannel() throws Exception {

        Context chContext = new Context();
        chContext.put("capacity", "2");
        chContext.put("transactionCapacity", "2");
        chContext.put("keep-alive", "0");
        channel.stop();
        Configurables.configure(channel, chContext);

        channel.start();
        Context context = new Context();
        File f1 = new File(tmpDir.getAbsolutePath() + "/file1");

        Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                        "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
                f1, Charsets.UTF_8);


        context.put(DirectorySourceConfigurationConstants.DIRECTORY,
                tmpDir.getAbsolutePath());
        context.put(DirectorySourceConfigurationConstants.TRACKER_DIR,
                trackDir.getAbsolutePath());

        context.put(DirectorySourceConfigurationConstants.BATCH_SIZE, "2");
        Configurables.configure(source, context);
        source.setBackOff(false);
        source.start();

        // Wait for the source to read enough events to fill up the channel.
        while(!source.hitChannelException()) {
            Thread.sleep(50);
        }

        List<String> dataOut = Lists.newArrayList();

        for (int i = 0; i < 8; ) {
            Transaction tx = channel.getTransaction();
            tx.begin();
            Event e = channel.take();
            if (e != null) {
                dataOut.add(new String(e.getBody(), "UTF-8"));
                i++;
            }
            e = channel.take();
            if (e != null) {
                dataOut.add(new String(e.getBody(), "UTF-8"));
                i++;
            }
            tx.commit();
            tx.close();
        }
        Assert.assertTrue("Expected to hit ChannelException, but did not!",
                source.hitChannelException());
        Assert.assertEquals(8, dataOut.size());
        source.stop();
    }

    @Test
    public void testEndWithZeroByteFiles() throws IOException, InterruptedException {
        Context context = new Context();

        File f1 = new File(tmpDir.getAbsolutePath() + "/file1");

        Files.write("file1line1\n", f1, Charsets.UTF_8);

        File f2 = new File(tmpDir.getAbsolutePath() + "/file2");
        File f3 = new File(tmpDir.getAbsolutePath() + "/file3");
        File f4 = new File(tmpDir.getAbsolutePath() + "/file4");

        Files.touch(f2);
        Files.touch(f3);
        Files.touch(f4);

        context.put(DirectorySourceConfigurationConstants.DIRECTORY,
                tmpDir.getAbsolutePath());
        context.put(DirectorySourceConfigurationConstants.TRACKER_DIR,
                trackDir.getAbsolutePath());
        Configurables.configure(source, context);
        source.start();

        // Need better way to ensure all files were processed.
        Thread.sleep(5000);

        Assert.assertFalse("Server did not error", source.hasFatalError());
        Assert.assertEquals("One message was read", 1,
                source.getSourceCounter().getEventAcceptedCount());
        source.stop();
    }
}
