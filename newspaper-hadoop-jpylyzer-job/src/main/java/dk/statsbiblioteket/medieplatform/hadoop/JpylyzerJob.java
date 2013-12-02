package dk.statsbiblioteket.medieplatform.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class JpylyzerJob implements Tool {

    private Configuration conf;

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new JpylyzerJob(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(JpylyzerJob.class);
        job.setMapperClass(JpylyzerMapper.class);

        Configuration configuration = job.getConfiguration();
        configuration.setIfUnset(ConfigConstants.JPYLYZER_PATH, "jpylyzer.py");
        configuration.setIfUnset(ConfigConstants.DOMS_URL, "http://achernar:7880/fedora");
        configuration.setIfUnset(ConfigConstants.DOMS_USERNAME, "fedoraAdmin");
        configuration.setIfUnset(ConfigConstants.DOMS_PASSWORD, "fedoraAdminPass");


        job.setReducerClass(DomsSaverReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean result = job.waitForCompletion(true);
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
