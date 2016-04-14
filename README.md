# flume-source-directorysource
a source plugin for apache flume use spooldir as an reference but improved in many ways

# An alternative to the spooldir source from apache-flume
there are a few limitation for spooldir:

* have to move or delete files that been digesting
* can not recursively read files in sub-directory that been watched

this plugin watch every directories for new file in the given directory use JDK's buildin API: WatchService.
also, it read exiting files if the setting of "startFrom" equals "BEGINNING".
all files are tracked by sqllite database, so don't have to worry about content been skipped or been read twice.

here is an config example:

    # Name the components on this agent
    a1.sources = r1
    a1.channels = c1
    a1.sinks = k1

    # Describe/configure the source
    a1.sources.r1.type = org.apache.flume.source.DirectorySource
    a1.sources.r1.channels = c1
    a1.sources.r1.Dir = /logs
    a1.sources.r1.trackerDir = /sincedb
    a1.sources.r1.fileHeader = true

    # Describe the sink
    a1.sinks.k1.type = logger

    # Use a channel which buffers events in memory
    a1.channels.c1.type = memory
    a1.channels.c1.capacity = 1000
    a1.channels.c1.transactionCapacity = 100
    
    # Bind the source and sink to the channel
    a1.sources.r1.channels = c1
    a1.sinks.k1.channel = c1
 
  
test with apache-flume-ng:

    bin/flume-ng agent -n a1 -c conf -f sample.conf

you can find the logs in

    logs/flume.log