1.8
* Use newest version of item event framework. No functional changes for this module.
* Configuration has been extended and changed and example config has been updated. Please update your configuration files.

1.7
* Update to batch event framework 1.10

1.6
* Change hadoop client dependencies to Cloudera branded 2.0.0-cdh4.5.0

1.5
* Update the hadoop client libraries to 2.0.3-alpha to include fix for known bug: https://issues.apache.org/jira/browse/MAPREDUCE-4782

1.4
* Removed explicit dependency on doms central, as this broke the batch event framework, which depends
on a newer version

1.3
* Updated to newspaper-parent 1.2
* Updated to version 1.6 of batch event framework

1.2.2
*Updated framework dependency to 1.4.5 to enable maxResults

1.2.1
* Update to newspaper-batch-event-framework 1.4.2 to make the component quiet on stderr
* Remove calls to System.out.println from non-test code

1.2
Second Release
Restructured to use the more generic components from the batch event framework

1.1 2013/12/11
Initial release
 - Works as a autonomous component
 - Programmatic job submission to the cluster
