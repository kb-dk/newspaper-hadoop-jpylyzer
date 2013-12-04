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

/**
 * This reducer saves the result in doms
 * The input is filepath, jpylyzer xml output
 * The output is filepath, domspid
 */
public class DomsSaverReducer extends Reducer<Text, Text, Text, Text> {


    private EnhancedFedora fedora;
    private String batchID = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        System.out
              .println("Setup called for reducer");
        fedora = createFedoraClient(context);
        batchID = context.getConfiguration()
                         .get(ConfigConstants.BATCH_ID);
        System.out
              .println("batchID read as " + batchID);

    }

    /**
     * Get the fedora client
     *
     * @param context the hadoop context
     *
     * @return the fedora client
     * @throws IOException
     */
    protected EnhancedFedora createFedoraClient(Context context) throws IOException {
        try {
            Configuration conf = context.getConfiguration();

            String username = conf.get(dk.statsbiblioteket.medieplatform.autonomous.ConfigConstants.DOMS_USERNAME);
            String password = conf.get(dk.statsbiblioteket.medieplatform.autonomous.ConfigConstants.DOMS_PASSWORD);
            String domsUrl = conf.get(dk.statsbiblioteket.medieplatform.autonomous.ConfigConstants.DOMS_URL);
            System.out
                  .println(username);
            System.out
                  .println(password);
            System.out
                  .println(domsUrl);
            return new EnhancedFedoraImpl(
                    new Credentials(username, password), domsUrl, null, null);
        } catch (JAXBException e) {
            throw new IOException(e);
        } catch (PIDGeneratorException e) {
            throw new IOException(e);
        }
    }

    /**
     * Reducde (save result in doms)
     *
     * @param key
     * @param values
     * @param context
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String pid = getDomsPid(key);
        System.out
              .println(pid);
        String translate = translate(key.toString());
        boolean first = false;
        for (Text value : values) {
            if (!first) {
                first = true;
            } else {
                throw new RuntimeException("Found multiple jpylyzer results for file '" + translate + "'");
            }
            try {
                fedora.modifyDatastreamByValue(
                        pid,
                        "JPYLYZER",
                        value.toString(),
                        Arrays.asList(translate + ".jpylyzer.xml"),
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

    /**
     * Get the doms pid from the filename
     *
     * @param key the filename
     *
     * @return the doms pid
     * @throws IOException
     */
    private String getDomsPid(Text key) throws IOException {
        try {
            String filePath = translate(key.toString());
            String path = "path:" + filePath;
            List<String> hits = fedora.findObjectFromDCIdentifier(path);
            if (hits.isEmpty()) {

                throw new RuntimeException("Failed to look up doms object for DC identifier '" + path + "'");
            } else {
                if (hits.size() > 1) {
                    System.err
                          .println("Found multipe pids for dc identifier '"+path+"'");
                }
                return hits.get(0);
            }
        } catch (BackendMethodFailedException e) {
            throw new IOException(e);
        } catch (BackendInvalidCredsException e) {
            throw new IOException(e);
        }
    }

    /**
     * Translate the filename back to the original path as stored in doms
     *
     * @param file the filename
     *
     * @return the original path
     */
    private String translate(String file) {
        return file.substring(file.indexOf(batchID))
                   .replaceAll("_", "/");
    }


}
