package dk.statsbiblioteket.medieplatform.autonomous;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/** This is a sample component to serve as a guide to developers */
public class JpylyzerComponent {

    private static Logger log = LoggerFactory.getLogger(JpylyzerComponent.class);

    /**
     * The class must have a main method, so it can be started as a command line tool
     *
     * @param args the arguments.
     *
     * @throws Exception
     * @see SBOIDomsAutonomousComponentUtils#parseArgs(String[])
     */
    public static void main(String[] args)
            throws
            Exception {
        log.info("Starting with args {}", args);

        //Parse the args to a properties construct
        Properties properties = SBOIDomsAutonomousComponentUtils.parseArgs(args);

        //make a new runnable component from the properties
        RunnableComponent component = new JpylyzerRunnableComponent(properties);

        CallResult result = SBOIDomsAutonomousComponentUtils.startAutonomousComponent(properties, component);
        log.info(result.toString());
        System.exit(result.containsFailures());
    }
}
