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
     * true iff we appended all the entries included in the sender's request
     */
    public boolean successfulAppend;

    /**
     * Index of the next log entry to send to us
     */
    public int nextIndex;
    /**
     * @param serverId          see RaftMessage.java
     * @param term              see RaftMessage.java
     * @param successfulAppend  see top of class file
     * @param nextIndex         see top of class file
     */
    public AppendEntriesReply(String serverId, int term,
            boolean successfulAppend, int nextIndex) {
        super(serverId, term);
        this.successfulAppend = successfulAppend;
        this.nextIndex = nextIndex;
    }
    
    @Override
    public String toString() {
        return "AppendEntriesReply [successfulAppend=" + successfulAppend
                + ", nextIndex=" + nextIndex + ", serverId=" + serverId
                + ", term=" + term + "]";
    }

}
