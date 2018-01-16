/*
 * Initiated and sent by candidates to gather votes ($5.2)
 */
public class RequestVoteRequest extends Message {
    int term;           // candidate's term
    int candidateId;    // candidate requesting vote
    int lastLogIndex;   // index of candidate's last log entry ($5.4)
    int lastLogTerm;    // term of candidate's last log entry ($5.4)
    @Override
    public String toString() {
        return "RequestVoteRequest [term=" + term + ", candidateId="
                + candidateId + ", lastLogIndex=" + lastLogIndex
                + ", lastLogTerm=" + lastLogTerm + "]";
    }
    public RequestVoteRequest(String serverId, int term, int candidateId, int lastLogIndex,
            int lastLogTerm) {
        super();
        this.serverId = serverId;
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }
}
