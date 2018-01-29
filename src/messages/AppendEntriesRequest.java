package messages;

import misc.LogEntry;

/*
 * Invoked by leader to replicate log entries ($5.3); also used as
 * heartbeat ($5.2).
 */
@SuppressWarnings("serial")
public class AppendEntriesRequest extends Message {
    public int prevLogIndex; // index of log entry immediately preceding new ones
    public int prevLogTerm;  // term of prevLogIndex entry
    public LogEntry entry;   // log entries to store (empty for heartbeat;
                      // may send more than one for efficiency)
    public int leaderCommit; //leader's commitIndex
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
