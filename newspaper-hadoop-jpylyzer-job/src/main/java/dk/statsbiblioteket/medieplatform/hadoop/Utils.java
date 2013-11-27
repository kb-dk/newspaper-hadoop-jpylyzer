package dk.statsbiblioteket.medieplatform.hadoop;

import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class Utils {
    static Text asText(InputStream inputStream) throws IOException {
        StringBuilder builder = new StringBuilder();
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        while ((line = reader.readLine()) != null){
            builder.append(line);
        }
        reader.close();
        return new Text(builder.toString());
    }
}
