import java.util.Arrays;

/*
 * Invoked by leader to replicate log entries ($5.3); also used as
 * heartbeat ($5.2).
 */
public class AppendEntriesRequest extends Message {
    int term;           // leader's term
    int leaderId;       // so follower can redirect clients
    int prevLogIndex;   // index of log entry immediately preceding new ones
    int prevLogTerm;    // term of prevLogIndex entry
    String[] entries;   // log entries to store (empty for heartbeat;
                        // may send more than one for efficiency)
    int leaderCommit;   //leader's commitIndex
    public AppendEntriesRequest(int term, int leaderId, int prevLogIndex,
            int prevLogTerm, String[] entries, int leaderCommit) {
        super();
        this.type = "AppendEntriesRPC";
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries;
        this.leaderCommit = leaderCommit;
    }
    @Override
    public String toString() {
        return "AppendEntriesRequest [term=" + term + ", leaderId=" + leaderId
                + ", prevLogIndex=" + prevLogIndex + ", prevLogTerm="
                + prevLogTerm + ", entries=" + Arrays.toString(entries)
                + ", leaderCommit=" + leaderCommit + ", type=" + type + "]";
    }
}
