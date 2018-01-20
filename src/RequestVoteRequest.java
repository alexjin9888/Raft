/*
 * Initiated and sent by candidates to gather votes ($5.2)
 */
public class RequestVoteRequest extends Message {
    int lastLogIndex;   // index of candidate's last log entry ($5.4)
    int lastLogTerm;    // term of candidate's last log entry ($5.4)
    @Override
    public String toString() {
        return "RequestVoteRequest [term=" + term + ", serverId="
                + serverId + ", lastLogIndex=" + lastLogIndex
                + ", lastLogTerm=" + lastLogTerm + "]";
    }
    public RequestVoteRequest(String serverId, int term, int lastLogIndex,
        int lastLogTerm) {
        super();
        this.serverId = serverId;
        this.term = term;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }
}
