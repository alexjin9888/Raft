package messages;

import java.util.ArrayList;

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
    // log entries for the recipient to append (zero or more)
    public ArrayList<LogEntry> entries;
    // leader's commitIndex
    public int leaderCommit;
    /**
     * @param serverId     see RaftMessage.java
     * @param term         see RaftMessage.java
     * @param prevLogIndex see top of class file
     * @param prevLogTerm  see top of class file
     * @param entries      see top of class file
     * @param leaderCommit see top of class file
     */
    public AppendEntriesRequest(String serverId, int term, int prevLogIndex,
        int prevLogTerm, ArrayList<LogEntry> entries, int leaderCommit) {
        super(serverId, term);
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries;
        this.leaderCommit = leaderCommit;
    }
    @Override
    public String toString() {
        return "AppendEntriesRequest [prevLogIndex=" + prevLogIndex
                + ", prevLogTerm=" + prevLogTerm + ", entries=" + entries
                + ", leaderCommit=" + leaderCommit + ", serverId=" + serverId
                + ", term=" + term + "]";
    }

}
