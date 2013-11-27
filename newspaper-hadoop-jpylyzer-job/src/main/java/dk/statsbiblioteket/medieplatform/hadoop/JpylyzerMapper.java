package dk.statsbiblioteket.medieplatform.hadoop;

import dk.statsbiblioteket.util.console.ProcessRunner;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

public class JpylyzerMapper extends Mapper<LongWritable,Text,Text,Text> implements Configurable {

    private Configuration configuration;

	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(value, asText(jpylize(new Path(value.toString()))));
    }

    private Text asText(InputStream inputStream) throws IOException {
        StringBuilder builder = new StringBuilder();
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        while ((line = reader.readLine()) != null){
            builder.append(line);
        }
        reader.close();
        return new Text(builder.toString());
    }

    /**
     * run jpylyzer on the given file and return the xml report as an inputstream.
     *
     * @param dataPath the path to the jp2 file
     *
     * @return the jpylyzer xml report
     * @throws IOException if the execution of jpylyzer failed in some fashion (not invalid file, if the program
     *                     returned non-zero returncode)
     */
    private InputStream jpylize(Path dataPath) throws IOException {
        ProcessRunner runner = new ProcessRunner(configuration.get("jpylyzerPath"), dataPath.toString());
        Map<String, String> myEnv = getJenkinsEnvironment();
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

    private Map<String, String> getJenkinsEnvironment() {
        Map<String, String> sysEnv = System.getenv();
        return sysEnv;
    }

	@Override
	public void setConf(Configuration conf) {
		this.configuration = conf; 
	}

	@Override
	public Configuration getConf() {
		return configuration;
	}
}
