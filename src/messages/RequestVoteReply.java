package messages;
/*
 * Reply to RequestVoteRequests
 */
@SuppressWarnings("serial")
public class RequestVoteReply extends Message {
    public boolean voteGranted;    // true means candidate received vote

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
