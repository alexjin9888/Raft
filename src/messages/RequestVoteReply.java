package messages;
/**
 * Reply to RequestVote request
 *
 */
@SuppressWarnings("serial")
public class RequestVoteReply extends Message {
    /**
     * See RAFT figure 2 for explanation of these variables
     */
    public boolean voteGranted;

    /**
     * @param serverId    ID of server sending the reply
     * @param term        my current term
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
