package messages;
import java.io.Serializable;

/**
 * Class used to house common fields of different messages that are sent and
 * received among servers.
 */
@SuppressWarnings("serial")
public abstract class Message implements Serializable {
    public String serverId; // ID of the server sending this message
    public int term; // currentTerm of the sender
}
