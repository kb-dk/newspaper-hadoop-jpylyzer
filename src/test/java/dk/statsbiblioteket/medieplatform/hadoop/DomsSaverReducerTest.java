package dk.statsbiblioteket.medieplatform.hadoop;

import dk.statsbiblioteket.doms.central.connectors.BackendInvalidCredsException;
import dk.statsbiblioteket.doms.central.connectors.BackendInvalidResourceException;
import dk.statsbiblioteket.doms.central.connectors.BackendMethodFailedException;
import dk.statsbiblioteket.doms.central.connectors.EnhancedFedora;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DomsSaverReducerTest {

    @Test
    public void testSimplest() throws
                               IOException,
                               BackendInvalidCredsException,
                               BackendMethodFailedException,
                               BackendInvalidResourceException {
        ReduceDriver<Text, Text, Text, Text> reduceDriver;

        EnhancedFedora fedora = mock(EnhancedFedora.class);
        String testPid = "uuid:testPid";
        when(fedora.findObjectFromDCIdentifier(anyString())).thenReturn(Arrays.asList(testPid));
        doNothing().when(fedora).modifyDatastreamByValue(eq(testPid), eq("JPYLYZER"), anyString(), anyList(), anyString());
        reduceDriver = ReduceDriver.newReduceDriver(new DomsSaverReducer(fedora));
        reduceDriver.withInput(new Text("testFile"),Arrays.asList(new Text("<jpylyzer/>")));
        reduceDriver.withOutput(new Text("testFile"),new Text(testPid));
        reduceDriver.runTest();
    }

}
