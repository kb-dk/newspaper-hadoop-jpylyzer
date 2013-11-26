package dk.statsbiblioteket.medieplatform.hadoop;

import dk.statsbiblioteket.doms.central.connectors.BackendInvalidCredsException;
import dk.statsbiblioteket.doms.central.connectors.BackendInvalidResourceException;
import dk.statsbiblioteket.doms.central.connectors.BackendMethodFailedException;
import dk.statsbiblioteket.doms.central.connectors.EnhancedFedora;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

public class DomsSaverReducer extends Reducer<Text,Text,Text,Text> {


    EnhancedFedora fedora;

    public DomsSaverReducer(EnhancedFedora fedora) {
        //TODO this constructure is illegal, must be empty
        this.fedora = fedora;
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String pid = getDomsPid(key);
        for (Text value : values) {
            try {
                fedora.modifyDatastreamByValue(pid,"JPYLYZER",value.toString(),null,"added Jpylyzer from Hadoop");
                context.write(key,new Text(pid));
            } catch (BackendInvalidCredsException | BackendMethodFailedException | BackendInvalidResourceException e) {
                throw new IOException(e);
            }
        }
    }

    private String getDomsPid(Text key) throws IOException {
        try {
            List<String> hits = fedora.findObjectFromDCIdentifier("path: "+translate(key.toString()));
            if (hits.isEmpty()){
                return null;
            } else {
                return hits.get(0);
            }
        } catch (BackendInvalidCredsException | BackendMethodFailedException e) {
            throw new IOException(e);
        }
    }

    private String translate(String s) {
        return s;
    }
}
