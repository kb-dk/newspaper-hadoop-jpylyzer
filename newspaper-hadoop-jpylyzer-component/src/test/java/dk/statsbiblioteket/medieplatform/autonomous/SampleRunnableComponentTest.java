package dk.statsbiblioteket.medieplatform.autonomous;

import org.testng.annotations.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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
        properties.setProperty(dk.statsbiblioteket.medieplatform.hadoop.ConfigConstants.PREFIX,"jpeg2k/");
        properties.setProperty(ConfigConstants.ITERATOR_USE_FILESYSTEM,"False");
        properties.setProperty(dk.statsbiblioteket.medieplatform.hadoop.ConfigConstants.FS_DEFAULT_NAME, "hdfs://phd-stage-master-01.statsbiblioteket.dk:8020");
        properties.setProperty(dk.statsbiblioteket.medieplatform.hadoop.ConfigConstants.YARN_RESOURCEMANAGER_ADDRESS, "phd-stage-master-01.statsbiblioteket.dk:8032");
        properties.setProperty(dk.statsbiblioteket.medieplatform.hadoop.ConfigConstants.HADOOP_USER, "newspapr");

        //properties.setProperty(ConfigConstants.JPYLYZER_PATH,"/usr/lib/python2.7/site-packages/jpylyzer/jpylyzer.py");
        properties.setProperty(ConfigConstants.JPYLYZER_PATH,"echo");


        JpylyzerRunnableComponent component = new JpylyzerRunnableComponent(properties){
            @Override
            public InputStream retrieveBatchStructure(Batch batch) throws IOException {
                return Thread.currentThread().getContextClassLoader().getResourceAsStream("assumed-valid-structure.xml");
            }
        };
        ResultCollector resultCollector = new ResultCollector("crap", "crap");

        component.doWorkOnBatch(batch, resultCollector);
        assertTrue(resultCollector.isSuccess(),resultCollector.toReport());
    }
}
