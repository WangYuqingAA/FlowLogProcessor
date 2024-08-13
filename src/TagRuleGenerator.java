import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Generates random tag rules and writes them to a CSV file.
 */
public class TagRuleGenerator {

    private static final Logger LOGGER = Logger.getLogger(TagRuleGenerator.class.getName());

    private static final String[] PROTOCOLS = {
        "ICMP", "TCP", "UDP", "IPv6", "GRE", "ESP", "AH", "ICMPv6", "OSPF"
    };

    private static final String[] CATEGORIES = {"SecurityGroup", "Environment", "Application", "Service"};
    private static final String[] ENVIRONMENTS = {"Prod", "Dev", "Test", "Staging"};
    private static final String[] APPLICATIONS = {"App1", "App2", "App3", "App4"};
    private static final String[] SERVICES = {"Web", "Database", "Cache", "Messaging"};

    /**
     * Generates a specified number of random tag rules and writes them to a CSV file.
     *
     * @param numberOfRecords the number of records to generate
     * @param csvFile the path of the CSV file to write to
     */
    public void generateTagRules(int numberOfRecords, String csvFile) {
        Set<DstAddrAndProtocol> uniqueCombinations = new HashSet<>();
        StringBuilder csvContent = new StringBuilder("dstport,protocol,tag\n");

        for (int i = 0; i < numberOfRecords; i++) {
        
            int dstPort;
            String protocol;
            DstAddrAndProtocol dstAddrAndProtocol;

            // Ensure unique dstPort-protocol combinations
            do {
                dstPort = randomPort();
                protocol = randomProtocol();
                dstAddrAndProtocol = new DstAddrAndProtocol(String.valueOf(dstPort), protocol);
            } while (uniqueCombinations.contains(dstAddrAndProtocol));

            uniqueCombinations.add(dstAddrAndProtocol);

            String tag = randomTag();

            csvContent.append(String.format("%d,%s,%s\n", dstPort, protocol, tag));
        }

        writeToFile(csvFile, csvContent.toString());
    }

    /**
     * Generates a random port number with a weighted probability distribution.
     *
     * @return a random port number
     */
    private int randomPort() {
        int probability = ThreadLocalRandom.current().nextInt(100);

        if (probability < 70) {
            return ThreadLocalRandom.current().nextInt(1, 1025);
        } else if (probability < 90) {
            return ThreadLocalRandom.current().nextInt(1025, 49152);
        } else {
            return ThreadLocalRandom.current().nextInt(49152, 65536);
        }
    }

    /**
     * Selects a random protocol from a predefined list.
     *
     * @return a random protocol
     */
    private String randomProtocol() {
        return PROTOCOLS[ThreadLocalRandom.current().nextInt(PROTOCOLS.length)];
    }

    /**
     * Generates a random tag based on predefined categories and values.
     *
     * @return a random tag
     */
    private String randomTag() {
        String category = CATEGORIES[ThreadLocalRandom.current().nextInt(CATEGORIES.length)];
        String tagValue = "";

        switch (category) {
            case "SecurityGroup":
                tagValue = "SG-" + ThreadLocalRandom.current().nextInt(1000);
                break;
            case "Environment":
                tagValue = ENVIRONMENTS[ThreadLocalRandom.current().nextInt(ENVIRONMENTS.length)];
                break;
            case "Application":
                tagValue = APPLICATIONS[ThreadLocalRandom.current().nextInt(APPLICATIONS.length)];
                break;
            case "Service":
                tagValue = SERVICES[ThreadLocalRandom.current().nextInt(SERVICES.length)];
                break;
        }
        return tagValue;
    }

    /**
     * Writes the generated content to a specified CSV file.
     *
     * @param csvFile the path of the CSV file to write to
     * @param content the content to write to the file
     */
    private void writeToFile(String csvFile, String content) {
        try (FileWriter writer = new FileWriter(csvFile)) {
            writer.write(content);
            LOGGER.info("Tag rules generated successfully!");
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Error writing to CSV file: " + csvFile, e);
            throw new RuntimeException("Failed to write CSV file: " + csvFile, e);
        }
    }
}
