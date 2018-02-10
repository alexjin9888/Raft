package messages;

import units.LogEntry;

/**
 * This class defines the message format of an AppendEntries request.
 */
public class AppendEntriesRequest extends RaftMessage {
    /**
     * Class versioning to support instance serialization/deserialization
     */
    private static final long serialVersionUID = 1L;

    // index of log entry immediately preceding new ones
    public int prevLogIndex;
    // term of prevLogIndex entry
    public int prevLogTerm;
    // log entry to store
    // Proj2: decide whether or not to send more than one entry
    public LogEntry entry;
    // leader's commitIndex
    public int leaderCommit;
    /**
     * @param serverId     see Message.java
     * @param term         see Message.java
     * @param prevLogIndex see above
     * @param prevLogTerm  see above
     * @param entry        see above
     * @param leaderCommit see above
     */
    public AppendEntriesRequest(String serverId, int term, int prevLogIndex,
        int prevLogTerm, LogEntry entry, int leaderCommit) {
        super(serverId, term);
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entry = entry;
        this.leaderCommit = leaderCommit;
    }
    @Override
    public String toString() {
        return "AppendEntriesRequest [prevLogIndex=" + prevLogIndex
                + ", prevLogTerm=" + prevLogTerm + ", entry=" + entry
                + ", leaderCommit=" + leaderCommit + ", serverId=" + serverId
                + ", term=" + term + "]";
    }

}
