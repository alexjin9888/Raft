package messages;
/**
 * This class defines the message format of a RequestVote request.
 */
public class RequestVoteRequest extends RaftMessage {
    /**
     * Class versioning to support instance serialization/deserialization
     */
    private static final long serialVersionUID = 1L;

    // index of candidate’s last log entry (§5.4)
    public int lastLogIndex;
    // term of candidate’s last log entry (§5.4)
    public int lastLogTerm;

    /**
     * @param serverId     See RaftMessage.java
     * @param term         See RaftMessage.java
     * @param lastLogIndex see above
     * @param lastLogTerm  see above
     */
    public RequestVoteRequest(String serverId, int term, int lastLogIndex,
        int lastLogTerm) {
        super(serverId, term);
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    @Override
    public String toString() {
        return "RequestVoteRequest [lastLogIndex=" + lastLogIndex
                + ", lastLogTerm=" + lastLogTerm + ", serverId=" + serverId
                + ", term=" + term + "]";
    }

}
