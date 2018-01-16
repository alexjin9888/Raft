/*
 * Reply to AppendEntriesRPCMessages   
 */
public class AppendEntriesReply extends Message {
    public AppendEntriesReply(int term, boolean success) {
        super();
        this.type = "AppendEntriesRPC";
        this.term = term;
        this.success = success;
    }
    @Override
    public String toString() {
        return "AppendEntriesReply [term=" + term + ", success=" + success
                + ", type=" + type + "]";
    }
    int term;           // currentTerm, for leader to update itself
    boolean success;    // true if  follower contained entry matching
                        // prevLogIndex and prevLogTerm
}
