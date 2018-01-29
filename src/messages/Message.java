package messages;
import java.io.Serializable;

/**
 * Abstract Class which includes common fields of different messages that are
 * sent and received among servers.
 */
@SuppressWarnings("serial")
public abstract class Message implements Serializable {
    /** 
     * serverId ID of the server sending this message
     */
    public String serverId;
    public int term; // currentTerm of the sender
}
