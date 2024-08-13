import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The {@code FlowLogGenerator} class generates synthetic flow log data and writes it to a CSV file.
 */
public class FlowLogGenerator {

    private static final Logger LOGGER = Logger.getLogger(FlowLogGenerator.class.getName());
    private static final Random RANDOM = new Random();
    private static final int BATCH_SIZE = 1000;
    private static final String CSV_HEADER = "version,account-id,interface-id,srcaddr,dstaddr,srcport,dstport,protocol,packets,bytes,start,end,action,log-status\n";
    private static final String[] PROTOCOLS = {"1", "6", "17", "41", "47", "50", "51", "58", "89"};
    private static final String[] LOG_STATUSES = {"OK", "NODATA", "SKIPDATA"};
    private static final int YEAR_IN_SECONDS = 31536000;

    /**
     * Generates a specified number of flow log records and writes them to a CSV file.
     *
     * @param numberOfRecords The number of flow log records to generate.
     * @param csvFile The path to the output CSV file.
     */
    public void generateFlowLogs(int numberOfRecords, String csvFile) {
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(csvFile))) {
            writer.write(CSV_HEADER);
            submitTasks(executor, writer, numberOfRecords);
            executor.shutdown();
            if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                LOGGER.warning("Executor did not terminate in the allotted time.");
            }
            LOGGER.info("Flow log data generated successfully!");
        } catch (IOException | InterruptedException e) {
            LOGGER.log(Level.SEVERE, "Error generating flow logs: " + csvFile, e);
            Thread.currentThread().interrupt(); // Restore interrupted status
        }
    }

    /**
     * Submits tasks to the executor for generating flow log records in batches.
     *
     * @param executor The executor service managing the tasks.
     * @param writer The writer used to output the CSV data.
     * @param numberOfRecords The total number of records to generate.
     */
    private void submitTasks(ExecutorService executor, BufferedWriter writer, int numberOfRecords) {
        for (int i = 0; i < numberOfRecords; i += BATCH_SIZE) {
            final int startIdx = i;
            executor.submit(() -> generateBatch(writer, numberOfRecords, startIdx));
        }
    }

    /**
     * Generates a batch of flow log records and writes them to the CSV file.
     *
     * @param writer The writer used to output the CSV data.
     * @param numberOfRecords The total number of records to generate.
     * @param startIdx The starting index for the current batch.
     */
    private void generateBatch(BufferedWriter writer, int numberOfRecords, int startIdx) {
        StringBuilder batch = new StringBuilder();
        for (int j = startIdx; j < startIdx + BATCH_SIZE && j < numberOfRecords; j++) {
            batch.append(generateSingleFlowLogRecord());
        }
        synchronized (writer) {
            try {
                writer.write(batch.toString());
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "Error writing batch to CSV file", e);
            }
        }
    }

    /**
     * Generates a single flow log record as a CSV string.
     *
     * @return A single flow log record.
     */
    private String generateSingleFlowLogRecord() {
        String version = "2";
        String accountId = generateRandomAccountId();
        String interfaceId = "eni-" + generateRandomHex(8);
        String srcAddr = generateRandomIp();
        String dstAddr = generateRandomIp();
        int srcPort = generateRandomPort();
        int dstPort = generateRandomPort();
        String protocol = generateRandomProtocol();
        int packets = generateRandomPackets();
        int bytes = estimateBytes(packets);
        long start = generateRandomTimestamp();
        long end = start + RANDOM.nextInt(600);
        String action = generateRandomAction();
        String logStatus = generateRandomLogStatus();

        return String.format("%s,%s,%s,%s,%s,%d,%d,%s,%d,%d,%d,%d,%s,%s\n",
                version, accountId, interfaceId, srcAddr, dstAddr, srcPort, dstPort,
                protocol, packets, bytes, start, end, action, logStatus);
    }

    // Random Value Generators

    /**
     * Generates a random private IPv4 address.
     *
     * @return A random private IPv4 address.
     */
    private String generateRandomIp() {
        int[] privateNetwork = {10, 172, 192};
        int firstOctet = privateNetwork[RANDOM.nextInt(privateNetwork.length)];
        int secondOctet = (firstOctet == 172) ? (16 + RANDOM.nextInt(16)) : RANDOM.nextInt(256);
        int thirdOctet = RANDOM.nextInt(256);
        int fourthOctet = RANDOM.nextInt(256);
        return String.format("%d.%d.%d.%d", firstOctet, secondOctet, thirdOctet, fourthOctet);
    }

    /**
     * Generates a random port number with a realistic distribution.
     *
     * @return A random port number.
     */
    private int generateRandomPort() {
        int probability = ThreadLocalRandom.current().nextInt(100);

        if (probability < 90) {
            return ThreadLocalRandom.current().nextInt(1, 1025); // Well-known ports
        } else if (probability < 95) {
            return ThreadLocalRandom.current().nextInt(1025, 49152); // Registered ports
        } else {
            return ThreadLocalRandom.current().nextInt(49152, 65536); // Dynamic/private ports
        }
    }

    /**
     * Generates a random hexadecimal string of the specified length.
     *
     * @param length The length of the hexadecimal string.
     * @return A random hexadecimal string.
     */
    private String generateRandomHex(int length) {
        StringBuilder hex = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            hex.append(Integer.toHexString(RANDOM.nextInt(16)));
        }
        return hex.toString();
    }

    /**
     * Generates a random 12-digit account ID.
     *
     * @return A random 12-digit account ID.
     */
    private String generateRandomAccountId() {
        StringBuilder accountId = new StringBuilder(12);
        for (int i = 0; i < 12; i++) {
            accountId.append(RANDOM.nextInt(10));
        }
        return accountId.toString();
    }

    /**
     * Selects a random protocol from a predefined list.
     *
     * @return A random protocol as a string.
     */
    private String generateRandomProtocol() {
        return PROTOCOLS[RANDOM.nextInt(PROTOCOLS.length)];
    }

    /**
     * Generates a random number of packets.
     *
     * @return A random number of packets.
     */
    private int generateRandomPackets() {
        return RANDOM.nextInt(1000) + 1;
    }

    /**
     * Estimates the number of bytes based on the number of packets.
     *
     * @param packets The number of packets.
     * @return An estimated number of bytes.
     */
    private int estimateBytes(int packets) {
        return packets * (RANDOM.nextInt(100) + 20);
    }

    /**
     * Generates a random timestamp within the last year.
     *
     * @return A random UNIX timestamp.
     */
    private long generateRandomTimestamp() {
        long currentTime = System.currentTimeMillis() / 1000L;
        return currentTime - RANDOM.nextInt(YEAR_IN_SECONDS);
    }

    /**
     * Randomly selects an action.
     *
     * @return The randomly selected action.
     */
    private String generateRandomAction() {
        return RANDOM.nextBoolean() ? "ACCEPT" : "REJECT";
    }

    /**
     * Randomly selects a log status from a predefined list.
     *
     * @return The randomly selected log status.
     */
    private String generateRandomLogStatus() {
        return LOG_STATUSES[RANDOM.nextInt(LOG_STATUSES.length)];
    }
}