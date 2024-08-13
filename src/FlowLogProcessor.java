import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The {@code FlowLogProcessor} class is responsible for processing flow logs and tag rules,
 * generating CSV files with counts of unique port-protocol combinations and tag occurrences.
 * 
 * <p>This class reads flow logs and tag rules from CSV files, processes the data, and produces
 * two output CSV files:
 * <ul>
 *   <li>A file that counts occurrences of unique port-protocol combinations.</li>
 *   <li>A file that counts occurrences of tags based on the flow logs and tag rules.</li>
 * </ul>
 * 
 * <p>Usage example:
 * <pre>{@code
 * FlowLogProcessor processor = new FlowLogProcessor();
 * List<DstAddrAndProtocol> dstAddrAndProtocols = processor.parseFlowLogs("flow_logs.csv");
 * processor.generatePortProtocolCountCsv(dstAddrAndProtocols, "port_protocol_counts.csv");
 * 
 * Map<DstAddrAndProtocol, String> tagRuleMap = processor.parseTagRules("tag_rules.csv");
 * processor.generateTagCountCsv(dstAddrAndProtocols, tagRuleMap, "tag_counts.csv");
 * }</pre>
 * 
 * @version 1.1
 */
public class FlowLogProcessor {

    private static final Logger logger = Logger.getLogger(FlowLogProcessor.class.getName());

    private static final Map<String, String> protocolMap = new ConcurrentHashMap<>();

    // Constants for CSV file headers and other strings
    private static final String CSV_HEADER_PORT_PROTOCOL_COUNT = "Port,Protocol,Count\n";
    private static final String CSV_HEADER_TAG_COUNT = "Tag,Count\n";
    private static final String UNTAGGED = "UNTAGGED";
    private static final String ERROR_READING_CSV = "Error reading CSV file: ";
    private static final String ERROR_WRITING_CSV = "Error writing to output CSV file: ";
    private static final String OUTPUT_SUCCESS = "Output CSV file generated successfully: ";


    // File path constants
    private static final String PORT_PROTOCOL_COUNTS = "port_protocol_counts.csv";
    private static final String FLOW_LOGS = "flow_logs.csv";
    private static final String TAG_RULES = "tag_rules.csv";
    private static final String TAG_COUNTS = "tag_counts.csv";

    static {
        protocolMap.put("1", "ICMP");
        protocolMap.put("6", "TCP");
        protocolMap.put("17", "UDP");
        protocolMap.put("41", "IPv6");
        protocolMap.put("47", "GRE");
        protocolMap.put("50", "ESP");
        protocolMap.put("51", "AH");
        protocolMap.put("58", "ICMPv6");
        protocolMap.put("89", "OSPF");
    }

    public static void main(String[] args) {
        TagRuleGenerator tagRuleGenerator = new TagRuleGenerator();
        FlowLogGenerator flowLogGenerator = new FlowLogGenerator();
        FlowLogProcessor processor = new FlowLogProcessor();

        flowLogGenerator.generateFlowLogs(100000, FLOW_LOGS);
        tagRuleGenerator.generateTagRules(10000, TAG_RULES);

        List<DstAddrAndProtocol> dstAddrAndProtocols = processor.parseFlowLogs(FLOW_LOGS);
        processor.generatePortProtocolCountCsv(dstAddrAndProtocols, PORT_PROTOCOL_COUNTS);

        Map<DstAddrAndProtocol, String> tagRuleMap = processor.parseTagRules(TAG_RULES);
        processor.generateTagCountCsv(dstAddrAndProtocols, tagRuleMap, TAG_COUNTS);
    }

    /**
     * Parses the flow logs CSV file and returns a list of {@code DstAddrAndProtocol} objects.
     *
     * @param csvFile The path to the flow logs CSV file.
     * @return A list of {@code DstAddrAndProtocol} objects representing port-protocol pairs.
     */
    public List<DstAddrAndProtocol> parseFlowLogs(String csvFile) {
        return readCsvLines(csvFile).parallelStream()
            .map(FlowLogProcessor::processLine)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }

    /**
     * Parses the tag rules CSV file and returns a map of {@code DstAddrAndProtocol} to tags.
     *
     * @param csvFile The path to the tag rules CSV file.
     * @return A map where the key is a {@code DstAddrAndProtocol} object and the value is the corresponding tag.
     */
    public Map<DstAddrAndProtocol, String> parseTagRules(String csvFile) {
        return readCsvLines(csvFile).parallelStream()
            .map(line -> line.split(","))
            .filter(values -> values.length >= 3)
            .collect(Collectors.toConcurrentMap(
                values -> new DstAddrAndProtocol(values[0], values[1]),
                values -> values[2]
            ));
    }

    /**
     * Generates a CSV file with counts of unique port-protocol combinations from flow logs.
     *
     * @param dstAddrAndProtocols The list of {@code DstAddrAndProtocol} objects to count.
     * @param outputCsvFile       The path to the output CSV file.
     */
    public void generatePortProtocolCountCsv(List<DstAddrAndProtocol> dstAddrAndProtocols, String outputCsvFile) {
        Map<DstAddrAndProtocol, Long> countMap = dstAddrAndProtocols.parallelStream()
            .collect(Collectors.groupingByConcurrent(dap -> dap, Collectors.counting()));

        writeCsv(outputCsvFile, CSV_HEADER_PORT_PROTOCOL_COUNT, countMap.entrySet().stream()
            .map(entry -> String.format("%s,%d\n", entry.getKey(), entry.getValue())));
    }

/**
     * Generates a CSV file with counts of tags based on flow logs and tag rules.
     *
     * @param dstAddrAndProtocols The list of {@code DstAddrAndProtocol} objects from flow logs.
     * @param tagRuleMap          The map of {@code DstAddrAndProtocol} to tags.
     * @param outputCsvFile       The path to the output CSV file.
     */
    public void generateTagCountCsv(List<DstAddrAndProtocol> dstAddrAndProtocols, 
                                    Map<DstAddrAndProtocol, String> tagRuleMap, 
                                    String outputCsvFile) {
        Map<String, Long> countMap = dstAddrAndProtocols.parallelStream()
            .map(dap -> tagRuleMap.getOrDefault(dap, UNTAGGED))
            .collect(Collectors.groupingByConcurrent(tag -> tag, Collectors.counting()));

        writeCsv(outputCsvFile, CSV_HEADER_TAG_COUNT, countMap.entrySet().stream()
            .map(entry -> String.format("%s,%d\n", entry.getKey(), entry.getValue())));
    }



    /**
     * Reads all lines from a CSV file, skipping the header.
     *
     * @param csvFile The path to the CSV file.
     * @return A list of lines from the CSV file.
     */
    private static List<String> readCsvLines(String csvFile) {
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            return br.lines().skip(1).collect(Collectors.toList());
        } catch (IOException e) {
            logger.log(Level.SEVERE, ERROR_READING_CSV + csvFile, e);
            throw new RuntimeException(ERROR_READING_CSV + csvFile, e);
        }
    }

    /**
     * Writes lines to a CSV file with the specified header.
     *
     * @param outputCsvFile The path to the output CSV file.
     * @param header        The header to be written at the top of the CSV file.
     * @param linesStream   A stream of lines to be written to the CSV file.
     */
    private static void writeCsv(String outputCsvFile, String header, Stream<String> linesStream) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputCsvFile))) {
            writer.write(header);
            linesStream.forEach(line -> {
                try {
                    writer.write(line);
                } catch (IOException e) {
                    logger.log(Level.SEVERE, ERROR_WRITING_CSV + outputCsvFile, e);
                    throw new RuntimeException(ERROR_WRITING_CSV + outputCsvFile, e);
                }
            });
            logger.info(OUTPUT_SUCCESS + outputCsvFile);
        } catch (IOException e) {
            logger.log(Level.SEVERE, ERROR_WRITING_CSV + outputCsvFile, e);
            throw new RuntimeException(ERROR_WRITING_CSV + outputCsvFile, e);
        }
    }

    private static DstAddrAndProtocol processLine(String line) {
        String[] values = line.split(",");
        if (values.length < 8) return null;

        String dstPort = values[6];
        String protocol = protocolMap.getOrDefault(values[7], null);

        return protocol != null ? new DstAddrAndProtocol(dstPort, protocol) : null;
    }
}