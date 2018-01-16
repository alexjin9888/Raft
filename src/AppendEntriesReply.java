/*
 * Reply to AppendEntriesRPCMessages   
 */
public class AppendEntriesReply extends Message {
    public AppendEntriesReply(String serverId, int term, boolean success) {
        super();
        this.serverId = serverId;
        this.term = term;
        this.success = success;
    }
    @Override
    public String toString() {
        return "AppendEntriesReply [term=" + term + ", success=" + success
                + "]";
    }
    int term;           // currentTerm, for leader to update itself
    boolean success;    // true if  follower contained entry matching
                        // prevLogIndex and prevLogTerm
}
