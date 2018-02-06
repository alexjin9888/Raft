package messages;
/**
 * This class defines the message format of an AppendEntries reply.
 */
public class AppendEntriesReply extends RaftMessage {
    /**
     * Class versioning to support instance serialization/deserialization
     */
    private static final long serialVersionUID = 1L;

    /** 
     * true iff follower contained entry matching prevLogIndex and prevLogTerm
     */
    public boolean successfulAppend;
    /**
     * @param serverId          see Message.java
     * @param term              see Message.java
     * @param successfulAppend  see above
     */
    public AppendEntriesReply(String serverId, int term, boolean successfulAppend) {
        super(serverId, term);
        this.successfulAppend = successfulAppend;
    }
    @Override
    public String toString() {
        return "AppendEntriesReply [term=" + term + ", successfulAppend="
                + successfulAppend + "]";
    }
}
