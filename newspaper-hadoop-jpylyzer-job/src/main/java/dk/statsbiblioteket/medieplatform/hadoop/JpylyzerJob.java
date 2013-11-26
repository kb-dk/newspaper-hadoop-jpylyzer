package dk.statsbiblioteket.medieplatform.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Dictionary;

public class JpylyzerJob {


    //TODO use for something
    public void runJob(String inputFiles, String outputFolder) throws
                                                               IOException,
                                                               ClassNotFoundException,
                                                               InterruptedException {
        Job job = Job.getInstance();
        job.setJarByClass(Dictionary.class);
        job.setMapperClass(JpylyzerMapper.class);
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
        System.exit(result ? 0 : 1);

    }

}
