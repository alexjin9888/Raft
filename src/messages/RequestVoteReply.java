package messages;
/**
 * This class defines the message format of a RequestVote reply.
 */
public class RequestVoteReply extends RaftMessage {
    /**
     * Class versioning to support instance serialization/deserialization
     */
    private static final long serialVersionUID = 1L;

    /**
     * Flag that indicates whether we grant the vote to the candidate
     */
    public boolean grantVote;

    /**
     * @param serverId    See RaftMessage.java
     * @param term        See RaftMessage.java
     * @param grantVote see above
     */
    public RequestVoteReply(String serverId, int term, boolean grantVote) {
        super(serverId, term);
        this.grantVote = grantVote;
    }

    @Override
    public String toString() {
        return "RequestVoteReply [grantVote=" + grantVote + ", serverId="
                + serverId + ", term=" + term + "]";
    }

}
