package dk.statsbiblioteket.medieplatform.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

public class JpylyzerMapperTest {


    @BeforeClass
    public void setUp() {
        //JpylyzerMapper mapper = new JpylyzerMapper("src/test/extras/jpylyzer-1.10.1/jpylyzer.py");
    }

    @Test
    public void testSimplest() throws IOException {
        MapDriver<LongWritable, Text, Text, Text> mapDriver;
        JpylyzerMapper mapper = new JpylyzerMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
        mapDriver.getConfiguration().set(dk.statsbiblioteket.medieplatform.autonomous.ConfigConstants.JPYLYZER_PATH, "echo");

        mapDriver.withInput(new LongWritable(1), new Text("ein"));
        mapDriver.withOutput(new Text("ein"), new Text("ein"));
        mapDriver.runTest();
    }

    @Test
    public void testMapper() throws IOException, URISyntaxException {

        MapDriver<LongWritable, Text, Text, Text> mapDriver;
        JpylyzerMapper mapper = new JpylyzerMapper();
        String name = "B400022028241-RT1/balloon.jp2";
        mapDriver = MapDriver.newMapDriver(mapper);
        File testFolder = new File(Thread.currentThread().getContextClassLoader().getResource(
                name).toURI()).getParentFile().getParentFile().getParentFile().getParentFile();
        File jpylyzerPath = new File(testFolder, "src/test/extras/jpylyzer-1.10.1/jpylyzer.py");
        mapDriver.getConfiguration().set(dk.statsbiblioteket.medieplatform.autonomous.ConfigConstants.JPYLYZER_PATH, jpylyzerPath.getAbsolutePath());


        String testFile = getAbsolutePath(name);
        mapDriver.withInput(new LongWritable(1), new Text(testFile));
        mapDriver.withOutput(new Text(testFile), Utils.asText(JpylyzerMapper.jpylize(testFile,jpylyzerPath.getAbsolutePath())));
        mapDriver.runTest();
    }

    private String getAbsolutePath(String name) throws URISyntaxException {
        return new File(Thread.currentThread().getContextClassLoader().getResource(
                name).toURI()).getAbsolutePath();
    }

}
