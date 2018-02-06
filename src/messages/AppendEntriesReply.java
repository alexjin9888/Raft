package messages;
/**
 * Instances of this class serve as reply to AppendEntries request
 */
@SuppressWarnings("serial")
public class AppendEntriesReply extends Message {
    /** 
     * true iff follower contained entry matching prevLogIndex and prevLogTerm
     */
    public boolean success;
    /**
     * @param serverId see Message.java
     * @param term     see Message.java
     * @param success  see above
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
