package dk.statsbiblioteket.medieplatform.hadoop;

import dk.statsbiblioteket.doms.central.connectors.BackendInvalidCredsException;
import dk.statsbiblioteket.doms.central.connectors.BackendInvalidResourceException;
import dk.statsbiblioteket.doms.central.connectors.BackendMethodFailedException;
import dk.statsbiblioteket.doms.central.connectors.EnhancedFedora;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;

import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This test class is meant to test the full Jpylyzer map reduce job.
 */
public class JPylyzerJobTest {
    MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;
    String testPid = "uuid:testPid";

    @Before
    public void setUp() throws BackendInvalidCredsException, BackendMethodFailedException,
            BackendInvalidResourceException, URISyntaxException {
        mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, Text>();

        JpylyzerMapper mapper = new JpylyzerMapper();
        File testFolder = new File(Thread.currentThread().getContextClassLoader().getResource("balloon.jp2").toURI()).getParentFile().getParentFile().getParentFile();
        File jpylyzerPath = new File(testFolder, "src/test/extras/jpylyzer-1.10.1/jpylyzer.py");
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.getConfiguration().set(ConfigConstants.JPYLYZER_PATH, jpylyzerPath.getAbsolutePath());

        final EnhancedFedora fedora = mock(EnhancedFedora.class);
        when(fedora.findObjectFromDCIdentifier(anyString())).thenReturn(Arrays.asList(testPid));
        doNothing().when(fedora).modifyDatastreamByValue(eq(testPid), eq("JPYLYZER"), anyString(), anyList(), anyString());

        mapReduceDriver.setReducer(new DomsSaverReducer() {
            @Override
            protected EnhancedFedora createFedoraClient(Context context) throws IOException {
                return fedora;
            }
        });
    }

    /**
     * Test the map and reduce steps together.
     * @throws IOException
     * @throws URISyntaxException
     */
    @Test
    public void testMapReduce() throws IOException, URISyntaxException {
        String name = "balloon.jp2";
        String testFile = getAbsolutePath(name);


        mapReduceDriver.withInput(new LongWritable(1), new Text(testFile));
        mapReduceDriver.addOutput(new Text(testFile), new Text(testPid));
        mapReduceDriver.runTest();
    }

    private String getAbsolutePath(String name) throws URISyntaxException {
        return new File(Thread.currentThread().getContextClassLoader().getResource(
                name).toURI()).getAbsolutePath();
    }

    @Test
    public void testJob() throws InterruptedException, IOException, ClassNotFoundException, URISyntaxException {
         //TODO test the Jpylyzer job class
        String fileListFileName = "jp2-file-list_balloon_balloon.txt";
        String fileListFile = getAbsolutePath(fileListFileName);
        String[] args = {fileListFile, "test-output"};
        JpylyzerJob.main(args);
    }
}
