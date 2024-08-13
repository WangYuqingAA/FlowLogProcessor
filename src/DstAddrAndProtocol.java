import java.util.Objects;

/**
 * The {@code DstAddrAndProtocol} class represents a combination of a destination port
 * and a protocol. It is used as a key in maps and sets for processing flow logs.
 * @version 1.0
 */
public class DstAddrAndProtocol {
    private final String dstPort;
    private final String protocol;

    public DstAddrAndProtocol(String dstPort, String protocol) {
        this.dstPort = dstPort;
        this.protocol = protocol;
    }

    public String getDstPort() {
        return dstPort;
    }

    public String getProtocol() {
        return protocol;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DstAddrAndProtocol that = (DstAddrAndProtocol) o;
        return dstPort.equals(that.dstPort) && protocol.equals(that.protocol);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dstPort, protocol);
    }

    @Override
    public String toString() {
        return dstPort + "," + protocol;
    }
}