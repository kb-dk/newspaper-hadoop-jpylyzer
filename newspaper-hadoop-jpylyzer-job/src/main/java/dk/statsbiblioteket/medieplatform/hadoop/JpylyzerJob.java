package dk.statsbiblioteket.medieplatform.hadoop;

import dk.statsbiblioteket.medieplatform.autonomous.ConfigConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;


/**
 * The jpylyzer job. Eats a text file containing paths to jpegs, runs jpylyzer on each and looks up the path in doms to
 * store the result.
 */
public class JpylyzerJob implements Tool {

    private static Logger log = Logger.getLogger(JpylyzerJob.class);
    private Configuration conf;

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new JpylyzerJob(), args);
        System.exit(res);
    }

    /**
     * Run the job with the args
     *
     * @param args first argument is a path to a file listing the jpeg2k files to work on. Second argument is to the
     *             output dir
     *
     * @return return code, 0 is success
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    @Override
    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = getConf();
        configuration.setIfUnset(ConfigConstants.JPYLYZER_PATH, "jpylyzer.py");
        configuration.setIfUnset(ConfigConstants.DOMS_URL, "http://achernar:7880/fedora");
        configuration.setIfUnset(ConfigConstants.DOMS_USERNAME, "fedoraAdmin");
        configuration.setIfUnset(ConfigConstants.DOMS_PASSWORD, "fedoraAdminPass");
        configuration.setIfUnset(DomsSaverReducer.HADOOP_SAVER_DATASTREAM,"JPYLYZER");

        Job job = Job.getInstance(configuration);
        job.setJobName("Newspaper " + getClass().getSimpleName() + " " + configuration.get(ConfigConstants.BATCH_ID));

        job.setJarByClass(JpylyzerJob.class);
        job.setMapperClass(JpylyzerMapper.class);
        job.setReducerClass(DomsSaverReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormatClass(NLineInputFormat.class);
        int filesPerMapTask = configuration.getInt(ConfigConstants.FILES_PER_MAP_TASK, 1);
        NLineInputFormat.setNumLinesPerSplit(job, filesPerMapTask);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        log.info(job);
        return result ? 0 : 1;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }
}
