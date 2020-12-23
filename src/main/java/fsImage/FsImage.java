package fsImage;

import org.apache.commons.cli.*;

import java.io.*;

import static org.apache.hadoop.hdfs.tools.offlineImageViewer.OfflineImageViewer.buildOptions;
import static org.apache.zookeeper.Version.printUsage;

/**
 * @author w9006271
 * @since 2020/12/10
 */

public class FsImage {
    private static final String HELP_OPT = "-h";
    private static final String HELP_LONGOPT = "--help";

    public static void main(String[] args){
        int run = run(args);
        System.out.println(run);
    }


    public static int run(String[] args){
        Options options = buildOptions();
        if (args.length == 0) {
            printUsage();
            return 0;
        }
        // print help and exit with zero exit code
        if (args.length == 1 && isHelpOption(args[0])) {
            printUsage();
            return 0;
        }
        CommandLineParser parser = new PosixParser();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println("Error parsing command-line options: ");
            printUsage();
            return -1;
        }

        if (cmd.hasOption("h")) {
            // print help and exit with non zero exit code since
            // it is not expected to give help and other options together.
            printUsage();
            return -1;
        }

        String inputFile = cmd.getOptionValue("i");
        String processor = cmd.getOptionValue("p", "Web");
        String delimiter = cmd.getOptionValue("delimiter",
                ",");
        String tempPath = cmd.getOptionValue("t", "");
        try {
            if (processor.equals("Delimited")) {
                try (PBImageDelimitedTextWriter writer =
                             new PBImageDelimitedTextWriter(delimiter, tempPath)) {
                    writer.visit(new RandomAccessFile(inputFile, "r"));
                }
            } else {
                System.err.println("Invalid processor specified : " + processor);
                printUsage();
                return -1;
            }
            return 0;
        } catch (EOFException e) {
            System.err.println("Input file ended unexpectedly. Exiting");
        } catch (IOException e) {
            System.err.println("Encountered exception.  Exiting: " + e.getMessage());
        }
        return 0;
    }


    private static boolean isHelpOption(String arg) {
        return arg.equalsIgnoreCase(HELP_OPT) ||
                arg.equalsIgnoreCase(HELP_LONGOPT);
    }

}
