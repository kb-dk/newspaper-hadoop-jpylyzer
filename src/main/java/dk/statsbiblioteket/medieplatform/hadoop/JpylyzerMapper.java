package dk.statsbiblioteket.medieplatform.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class JpylyzerMapper extends Mapper<LongWritable,Text,Text,Text> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        context.write(value,new Text("<jpylyzer/>"));
    }
}
