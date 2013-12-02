package dk.statsbiblioteket.medieplatform.hadoop;

import dk.statsbiblioteket.doms.central.connectors.BackendInvalidCredsException;
import dk.statsbiblioteket.doms.central.connectors.BackendInvalidResourceException;
import dk.statsbiblioteket.doms.central.connectors.BackendMethodFailedException;
import dk.statsbiblioteket.doms.central.connectors.EnhancedFedora;
import dk.statsbiblioteket.doms.central.connectors.EnhancedFedoraImpl;
import dk.statsbiblioteket.doms.central.connectors.fedora.pidGenerator.PIDGeneratorException;
import dk.statsbiblioteket.doms.webservices.authentication.Credentials;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class DomsSaverReducer extends Reducer<Text, Text, Text, Text> {


    private EnhancedFedora fedora;
    private String batchID = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        fedora = createFedoraClient(context);
        batchID = context.getConfiguration().get("batchID");

    }

    protected EnhancedFedora createFedoraClient(Context context) throws IOException {
        try {
            Configuration conf = context.getConfiguration();
            return new EnhancedFedoraImpl(
                    new Credentials(conf.get(ConfigConstants.DOMS_USERNAME), conf.get(ConfigConstants.DOMS_PASSWORD)),
                    conf.get(
                            ConfigConstants.DOMS_URL),
                    null,
                    null);
        } catch (JAXBException e) {
            throw new IOException(e);
        } catch (PIDGeneratorException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String pid = getDomsPid(key);
        for (Text value : values) {
            try {
                fedora.modifyDatastreamByValue(
                        pid,
                        "JPYLYZER",
                        value.toString(),
                        Arrays.asList(translate(key.toString()) + ".jpylyzer.xml"),
                        "added Jpylyzer output from Hadoop");
                context.write(key, new Text(pid));
            } catch (BackendInvalidCredsException e) {
                throw new IOException(e);
            } catch (BackendMethodFailedException e) {
                throw new IOException(e);
            } catch (BackendInvalidResourceException e) {
                throw new IOException(e);
            }

        }
    }

    private String getDomsPid(Text key) throws IOException {
        try {
            List<String> hits = fedora.findObjectFromDCIdentifier("path:" + translate(key.toString()));
            if (hits.isEmpty()) {
                System.out
                      .println("No pid found for path '"+translate(key.toString()+"'"));
                return null;
            } else {
                return hits.get(0);
            }
        } catch ( BackendMethodFailedException e) {
            throw new IOException(e);
        } catch (BackendInvalidCredsException  e) {
            throw new IOException(e);
        }
    }

    private String translate(String file) {
        return file.substring(file.indexOf(batchID))
                            .replaceAll("_", "/");
    }


}
