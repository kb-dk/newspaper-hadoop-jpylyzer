package dk.statsbiblioteket.medieplatform.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class JpylyzerMapperTest {
    MapDriver<LongWritable, Text, Text, Text> mapDriver;

    @Before
    public void setUp() {
        JpylyzerMapper mapper = new JpylyzerMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(1), new Text("ein"));
        mapDriver.withInput(new LongWritable(1), new Text("zwei"));
        mapDriver.withInput(new LongWritable(1), new Text("drei"));
        mapDriver.withOutput(new Text("ein"), new Text("<jpylyzer/>"));
        mapDriver.withOutput(new Text("zwei"), new Text("<jpylyzer/>"));
        mapDriver.withOutput(new Text("drei"), new Text("<jpylyzer/>"));
        mapDriver.runTest();
    }
}