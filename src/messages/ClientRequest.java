package messages;
import java.io.Serializable;
import java.net.InetSocketAddress;

/**
 * This class defines the message format of a client message.
 */
public class ClientRequest implements Serializable {
    /**
     * Class versioning to support instance serialization/deserialization
     */
    private static final long serialVersionUID = 1L;

    /**
     * Id that the client uses to differentiate between different commands
     */
    public int commandId;

    /**
     * Address of the client that sent this message
     */
    public InetSocketAddress clientAddress;
    /**
     * Shell command to be executed
     */
    public String command;

    /**
     * @param clientAddress see top of class file
     * @param command see top of class file
     */
    public ClientRequest(int commandId, InetSocketAddress clientAddress, 
            String command) {
        this.commandId = commandId;
        this.clientAddress = clientAddress;
        this.command = command;
    }

    @Override
    public String toString() {
        return "ClientRequest [commandId=" + commandId + ", clientAddress="
                + clientAddress + ", command=" + command + "]";
    }

}