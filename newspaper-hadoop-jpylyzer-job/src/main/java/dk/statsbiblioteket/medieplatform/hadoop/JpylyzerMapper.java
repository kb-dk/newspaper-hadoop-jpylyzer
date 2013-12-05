package dk.statsbiblioteket.medieplatform.hadoop;

import dk.statsbiblioteket.medieplatform.autonomous.ConfigConstants;
import dk.statsbiblioteket.util.console.ProcessRunner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Input is line-number, line text. The text is the path to a file to run jpylyzer on
 * Output is line text, jpylyzer output xml
 */
public class JpylyzerMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static Logger log = Logger.getLogger(JpylyzerJob.class);

    /**
     * run jpylyzer on the given file and return the xml report as an inputstream.
     *
     * @param dataPath     the path to the jp2 file
     * @param jpylyzerPath the path to the jpylyzer executable
     *
     * @return the jpylyzer xml report
     * @throws IOException if the execution of jpylyzer failed in some fashion (not invalid file, if the program
     *                     returned non-zero returncode)
     */
    protected static InputStream jpylize(String dataPath, String jpylyzerPath) throws IOException {
        ProcessRunner runner = new ProcessRunner(jpylyzerPath, dataPath);
        log.info("Running command '" + jpylyzerPath + " " + dataPath + "'");
        Map<String, String> myEnv = new HashMap<String, String>(System.getenv());
        runner.setEnviroment(myEnv);
        runner.setOutputCollectionByteSize(Integer.MAX_VALUE);

        //this call is blocking
        runner.run();

        //we could probably do something more clever with returning the output while the command is still running.
        if (runner.getReturnCode() == 0) {
            return runner.getProcessOutput();
        } else {
            String message
                    = "failed to run jpylyzer, returncode:" + runner.getReturnCode() + ", stdOut:" + runner.getProcessOutputAsString() + " stdErr:" + runner.getProcessErrorAsString();
            log.error(message);
            throw new IOException(message);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String jpylyzerPath = context.getConfiguration()
                                         .get(ConfigConstants.JPYLYZER_PATH);
            InputStream jpylize = jpylize(value.toString(), jpylyzerPath);
            Text text = Utils.asText(jpylize);
            context.write(value, text);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

}
