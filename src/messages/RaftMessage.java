package messages;
import java.io.Serializable;

/**
 * This class contains the fields included in all Raft messages.
 * One benefit of having this class is to enable common processing across all
 * Raft messages (e.g., perform term comparison in one location).
 */
public abstract class RaftMessage implements Serializable {
    /**
     * Class versioning to support instance serialization/deserialization
     */
    private static final long serialVersionUID = 1L;

    /**
     * ID of the server sending this message
     */
    public String serverId;
    /**
     * currentTerm of the sender
     */
    public int term;


    /**
     * @param serverId see top of class file
     * @param term     see top of class file
     */
    public RaftMessage(String serverId, int term) {
        this.serverId = serverId;
        this.term = term;
    }
}
