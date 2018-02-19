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
     * @param serverId    see RaftMessage.java
     * @param term        see RaftMessage.java
     * @param grantVote   see top of class file
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
