package dk.statsbiblioteket.medieplatform.autonomous;

import org.testng.annotations.Test;

import java.io.FileInputStream;
import java.util.Properties;

import static org.testng.Assert.assertTrue;

public class SampleRunnableComponentTest {
    @Test(groups = "integrationTest")
    public void testDoWorkOnBatch()
            throws
            Exception {
        String pathToProperties = System.getProperty("integration.test.newspaper.properties");
        Properties properties = new Properties();
        properties.load(new FileInputStream(pathToProperties));

        Batch batch = new Batch("400022028241");


        properties.setProperty(dk.statsbiblioteket.medieplatform.hadoop.ConfigConstants.JOB_FOLDER,"inputFiles");
        properties.setProperty(dk.statsbiblioteket.medieplatform.hadoop.ConfigConstants.PREFIX,"/net/zone1.isilon.sblokalnet/ifs/archive/bitmag-devel01-data/cache/avisbits/perm/avis/");
        properties.setProperty(dk.statsbiblioteket.medieplatform.hadoop.ConfigConstants.HADOOP_USER, "newspapr");
        properties.setProperty(ConfigConstants.JPYLYZER_PATH,"/usr/lib/python2.7/site-packages/jpylyzer/jpylyzer.py");


        JpylyzerRunnableComponent component = new JpylyzerRunnableComponent(properties);
        ResultCollector resultCollector = new ResultCollector("crap", "crap");

        component.doWorkOnBatch(batch, resultCollector);
        assertTrue(resultCollector.isSuccess(),resultCollector.toReport());
    }
}
