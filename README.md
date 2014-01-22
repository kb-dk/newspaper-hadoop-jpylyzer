Readme

This is an autonomous component that starts hadoop jobs for all batches that have been ingested to the bitrepository.
The hadoop job runs jpylyzer.py on the files, and saves the result in the corresponding object in DOMS.

It expects to find a config file of the name config.properties in the conf folder of the install dir

The config file must contain these values

#Doms
doms.username=
doms.password=
doms.url=http://<domsServer>:7880/fedora

#Batch iterator
iterator.useFileSystem=false

#Autonomous component framework
autonomous.lockserver.url=<zookeeperServer>
autonomous.sboi.url=<summaWsdl>
autonomous.pastSuccessfulEvents=Data_Archived
autonomous.pastFailedEvents=
autonomous.futureEvents=JPylyzed
autonomous.maxThreads=1
autonomous.maxRuntimeForWorkers=360000000

#hadoop
job.folder=<temp folder on HDFS>
file.storage.path=<prefix to get the files from the bit repository>
hadoop.user=newspapr
ninestars.jpylyzer.executable=/usr/lib/python2.7/site-packages/jpylyzer/jpylyzer.py
hadoop.files.per.map.tasks=5

Futhermore, the conf folder must contain the two files
core-site.xml
yarn-site.xml
These should be identical to the ones deployed on the hadoop cluster.

