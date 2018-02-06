package messages;

import units.LogEntry;

/**
 * Sent by leader to other servers to replicate log entries ($5.3); also used
 * as a heartbeat ($5.2).
 */
@SuppressWarnings("serial")
public class AppendEntriesRequest extends Message {
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
        super();
        this.serverId = serverId;
        this.term = term;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entry = entry;
        this.leaderCommit = leaderCommit;
    }
    @Override
    public String toString() {
        return "AppendEntriesRequest [term=" + term + ", serverId=" + serverId
                + ", prevLogIndex=" + prevLogIndex + ", prevLogTerm="
                + prevLogTerm + ", entry=" + entry
                + ", leaderCommit=" + leaderCommit + "]";
    }
}
