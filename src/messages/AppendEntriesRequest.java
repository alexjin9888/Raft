package messages;

import misc.LogEntry;

/**
 * Invoked by leader to replicate log entries ($5.3); also used as
 * heartbeat ($5.2).
 */
@SuppressWarnings("serial")
public class AppendEntriesRequest extends Message {
    // index of log entry immediately preceding new ones
    public int prevLogIndex;
    // term of prevLogIndex entry
    public int prevLogTerm;
    // log entry to store (empty for heartbeat; may send more than one for 
    //                     efficiency)
    public LogEntry entry;
    // leader’s commitIndex
    public int leaderCommit;
    /**
     * @param serverId     ID of the leader who is sending this request
     * @param term         My current term
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
