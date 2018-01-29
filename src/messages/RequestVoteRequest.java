package messages;
/*
 * Initiated and sent by candidates to gather votes ($5.2)
 */
@SuppressWarnings("serial")
public class RequestVoteRequest extends Message {
    public int lastLogIndex;   // index of candidate's last log entry ($5.4)
    public int lastLogTerm;    // term of candidate's last log entry ($5.4)

    public RequestVoteRequest(String serverId, int term, int lastLogIndex,
        int lastLogTerm) {
        super();
        this.serverId = serverId;
        this.term = term;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }
    @Override
    public String toString() {
        return "RequestVoteRequest [term=" + term + ", serverId="
                + serverId + ", lastLogIndex=" + lastLogIndex
                + ", lastLogTerm=" + lastLogTerm + "]";
    }
}
