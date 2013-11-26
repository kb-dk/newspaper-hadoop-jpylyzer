package dk.statsbiblioteket.medieplatform.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class PathInputFormat extends FileInputFormat<Path,Path> {


    @Override
    public RecordReader<Path, Path> createRecordReader(InputSplit split, TaskAttemptContext context) throws
                                                                                                     IOException,
                                                                                                     InterruptedException {

        return new RecordReader<Path, Path>() {

            private boolean unread = true;
            private Path filePath;

            @Override
            public void initialize(InputSplit split, TaskAttemptContext context) throws
                                                                                 IOException,
                                                                                 InterruptedException {

                filePath = ((FileSplit) split).getPath();
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                return unread;
            }

            @Override
            public Path getCurrentKey() throws IOException, InterruptedException {
                return filePath;
            }

            @Override
            public Path getCurrentValue() throws IOException, InterruptedException {
                return filePath;
            }

            @Override
            public float getProgress() throws IOException, InterruptedException {
                return unread? 0:1;
            }

            @Override
            public void close() throws IOException {

            }
        };
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}
