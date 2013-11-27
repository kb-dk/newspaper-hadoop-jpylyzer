package dk.statsbiblioteket.medieplatform.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class JpylyzerJob {
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException{
        int code = new JpylyzerJob().runJob(args[0], args[1]);
        System.exit(code);
	}

    //TODO use for something
    public int runJob(String inputFiles, String outputFolder) throws
                                                               IOException,
                                                               ClassNotFoundException,
                                                               InterruptedException {
        Job job = Job.getInstance();
        job.setJarByClass(JpylyzerJob.class);
        job.setMapperClass(JpylyzerMapper.class);
        
        Configuration configuration = job.getConfiguration();
        configuration.setIfUnset(ConfigConstants.JPYLYZER_PATH, "jpylyzer.py");
        configuration.setIfUnset(ConfigConstants.DOMS_URL,"http://achernar:7880/fedora");
        configuration.setIfUnset(ConfigConstants.DOMS_USERNAME,"fedoraAdmin");
        configuration.setIfUnset(ConfigConstants.DOMS_PASSWORD,"fedoraAdminPass");


        //job.setReducerClass(AllTranslationsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputFiles));
        FileOutputFormat.setOutputPath(job, new Path(outputFolder));
        boolean result = job.waitForCompletion(true);
        return result ? 0 : 1;
    }
}
