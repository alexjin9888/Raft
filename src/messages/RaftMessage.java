package messages;
import java.io.Serializable;

/**
 * This class contains the fields included in all Raft messages.
 */
public abstract class RaftMessage implements Serializable {
    /**
     * Class versioning to support instance serialization/deserialization
     */
    private static final long serialVersionUID = 1L;

    public String serverId; // ID of the server sending this message
    public int term; // currentTerm of the sender
    
    
    /**
     * @param serverId see above
     * @param term     see above
     */
    public RaftMessage(String serverId, int term) {
        this.serverId = serverId;
        this.term = term;
    }
}
