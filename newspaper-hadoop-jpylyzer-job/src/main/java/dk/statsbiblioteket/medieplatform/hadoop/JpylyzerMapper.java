package dk.statsbiblioteket.medieplatform.hadoop;

import dk.statsbiblioteket.util.console.ProcessRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class JpylyzerMapper extends Mapper<LongWritable,Text,Text,Text>  {


	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String jpylyzerPath = context.getConfiguration()
                                     .get(ConfigConstants.JPYLYZER_PATH);
        System.out
              .println(jpylyzerPath);
        InputStream jpylize = jpylize(new Path(value.toString()), jpylyzerPath);
        Text text = Utils.asText(jpylize);
        context.write(value, text);
    }


    /**
     * run jpylyzer on the given file and return the xml report as an inputstream.
     *
     *
     * @param dataPath the path to the jp2 file
     *
     * @return the jpylyzer xml report
     * @throws IOException if the execution of jpylyzer failed in some fashion (not invalid file, if the program
     *                     returned non-zero returncode)
     */
    protected static InputStream jpylize(Path dataPath, String jpylyzerPath) throws IOException {
        System.out
              .println("jpylyzing '"+dataPath+"' with '"+jpylyzerPath+"'");
        ProcessRunner runner = new ProcessRunner(jpylyzerPath, dataPath.toString());
        Map<String, String> myEnv = new HashMap<String,String>(System.getenv());
        runner.setEnviroment(myEnv);
        runner.setOutputCollectionByteSize(Integer.MAX_VALUE);

        //this call is blocking
        runner.run();

        //we could probably do something more clever with returning the output while the command is still running.
        if (runner.getReturnCode() == 0) {
            return runner.getProcessOutput();
        } else {
            throw new IOException("failed to run jpylyzer, returncode:" + runner.getReturnCode() + ", stdOut:"
                                  + runner.getProcessOutputAsString() + " stdErr:" + runner.getProcessErrorAsString());
        }
    }

}
