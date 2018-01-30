package messages;
/**
 * Reply to RequestVote request
 */
@SuppressWarnings("serial")
public class RequestVoteReply extends Message {
    // Flag that indicates whether we grant the vote to the candidate
    public boolean voteGranted;

    /**
     * @param serverId    see Message.java
     * @param term        see Message.java
     * @param voteGranted see above
     */
    public RequestVoteReply(String serverId, int term, boolean voteGranted) {
        super();
        this.serverId = serverId;
        this.term = term;
        this.voteGranted = voteGranted;
    }
    @Override
    public String toString() {
        return "RequestVoteReply [term=" + term + ", voteGranted=" + voteGranted
                + "]";
    }
}
