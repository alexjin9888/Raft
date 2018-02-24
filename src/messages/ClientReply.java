package messages;
import java.io.Serializable;
import java.net.InetSocketAddress;

/**
 * This class defines the message format of a reply to a client message.
 */
public class ClientReply implements Serializable {
    /**
     * Class versioning to support instance serialization/deserialization
     */
    private static final long serialVersionUID = 1L;

    /**
     * Id that the client uses to differentiate between different commands
     */
    public int commandId;
    /**
     * Address of the current leader in the Raft cluster
     */
    public InetSocketAddress leaderAddress;
    /**
     * True iff the command sent by the client was received by a leader who 
     * successfully executed the command.
     */
    public boolean success;
    /**
     * Result of the shell command executed
     */
    public String result;

    /**
     * @param leaderAddress see top of class file
     * @param success see top of class file
     * @param result see top of class file
     */
    public ClientReply(int commandId, InetSocketAddress leaderAddress, 
            boolean success, String result) {
        this.commandId = commandId;
        this.leaderAddress = leaderAddress;
        this.success = success;
        this.result = result;
    }

    @Override
    public String toString() {
        return "ClientReply [commandId=" + commandId + ", leaderAddress="
                + leaderAddress + ", success=" + success + ", result=" + 
                result + "]";
    }

}