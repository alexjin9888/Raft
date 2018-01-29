package messages;
/*
 * Reply to AppendEntriesRequest   
 */
@SuppressWarnings("serial")
public class AppendEntriesReply extends Message {
    public boolean success;    // true if  follower contained entry matching
                        // prevLogIndex and prevLogTerm
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
}
