package dk.statsbiblioteket.medieplatform.autonomous;

import dk.statsbiblioteket.medieplatform.hadoop.*;
import dk.statsbiblioteket.medieplatform.hadoop.ConfigConstants;
import dk.statsbiblioteket.util.xml.XSLT;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.transform.TransformerException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class JpylyzerRunnableComponent extends AbstractRunnableComponent {

    private static Logger log = LoggerFactory.getLogger(JpylyzerRunnableComponent.class);


    /**
     * Constructor matching super. Super requires a properties to be able to initialise the tree iterator, if needed.
     * If you do not need the tree iterator, ignore properties.
     *
     * You can use properties for your own stuff as well
     *
     * @param properties properties
     *
     * @see #getProperties()
     */
    public JpylyzerRunnableComponent(Properties properties) {
        super(properties);
    }

    @Override
    public String getEventID() {
        //This is the event ID that correspond to the work done by this component. It will be added to the list of
        //events a batch have experienced when the work is completed (along with information about success or failure)
        return "JPylyzed";
    }

    @Override
    public void doWorkOnBatch(Batch batch, ResultCollector resultCollector) throws Exception {

        //create the input as a file on the cluster
        Configuration conf = new Configuration();

        propertiesToConf(conf, getProperties());

        conf.set(dk.statsbiblioteket.medieplatform.hadoop.ConfigConstants.BATCH_ID, batch.getFullID());

        String user = conf.get(ConfigConstants.HADOOP_USER, "newspapr");
        FileSystem fs = FileSystem.get(FileSystem.getDefaultUri(conf), conf, user);

        //setup the dirs
        Path inputFile = new Path(
                getProperties().getProperty(dk.statsbiblioteket.medieplatform.hadoop.ConfigConstants.JOB_FOLDER),
                "input_" + batch.getFullID() + "_files.txt");
        Path outDir = new Path(
                getProperties().getProperty(dk.statsbiblioteket.medieplatform.hadoop.ConfigConstants.JOB_FOLDER),
                "output_" + batch.getFullID());

        //make file list stream from batch structure
        FSDataOutputStream fileoutStream = fs.create(
                inputFile);
        getFileList(batch, fileoutStream);
        fileoutStream.close();

        runJob(batch, resultCollector, conf, inputFile, outDir, user);


    }

    private void propertiesToConf(Configuration conf, Properties properties) {
        for (Map.Entry<Object, Object> objectObjectEntry : properties.entrySet()) {
            conf.setIfUnset(
                    objectObjectEntry.getKey()
                                     .toString(),
                    objectObjectEntry.getValue()
                                     .toString());
        }
    }

    private void runJob(final Batch batch, final ResultCollector resultCollector, final Configuration conf,
                        final Path inputFile, final Path outDir, String username) throws
                                                                                  IOException,
                                                                                  InterruptedException {
        //upload job to cluster if not already present
        //execute job on file

        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(username);
        ugi.doAs(
                new PrivilegedExceptionAction<ResultCollector>() {
                    public ResultCollector run() throws Exception {

                        Tool job = new JpylyzerJob();
                        job.setConf(conf);
                        try {
                            int result = ToolRunner.run(
                                    conf, job, new String[]{inputFile.toString(), outDir.toString()});
                            if (result != 0) {
                                resultCollector.addFailure(
                                        batch.getFullID(),
                                        "jp2file",
                                        getClass().getName(),
                                        "Failed to run on this batch");
                            }
                        } catch (Exception e) {
                            resultCollector.addFailure(
                                    batch.getFullID(), "exception", getClass().getName(), e.toString());
                        }
                        return resultCollector;
                    }
                });

    }

    private void getFileList(Batch batch, OutputStream outputStream) throws IOException, TransformerException {
        InputStream structure = retrieveBatchStructure(batch);
        HashMap<String, String> params = new HashMap<String, String>();
        params.put(
                dk.statsbiblioteket.medieplatform.hadoop.ConfigConstants.PREFIX,
                getProperties().getProperty(dk.statsbiblioteket.medieplatform.hadoop.ConfigConstants.PREFIX));
        XSLT.transform(
                Thread.currentThread()
                      .getContextClassLoader()
                      .getResource("fileNamesFromStructure.xslt"), structure, outputStream, params);

    }
}
