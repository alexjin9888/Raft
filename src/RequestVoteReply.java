/*
 * Reply to RequestVoteRPCMessages     
 */
public class RequestVoteReply extends Message {
    int term;               // currentTerm, for candidate to update itself
    boolean voteGranted;    // true means candidate received vote
    @Override
    public String toString() {
        return "RequestVoteReply [term=" + term + ", voteGranted=" + voteGranted
                + ", type=" + type + "]";
    }
}
