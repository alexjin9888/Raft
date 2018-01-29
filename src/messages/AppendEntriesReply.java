package messages;
/**
 * Reply to AppendEntriesRequest
 *
 */
@SuppressWarnings("serial")
public class AppendEntriesReply extends Message {
    /** 
     * true if  follower contained entry matching prevLogIndex and prevLogTerm
     * 
     */
    public boolean success;
    /**
     * @param serverId ID of server sending the reply
     * @param term     my current term
     * @param success  result of AppendEntriesRequest
     */
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
