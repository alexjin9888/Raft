package messages;
import java.io.Serializable;

/*
 * This is an message that is sent and received among servers.
 */
@SuppressWarnings("serial")
public abstract class Message implements Serializable {
	// serverId is candidateId in RequestVoteRequest
	//             leaderId in AppendEntriesRequest
    public String serverId;
    public int term; // currentTerm of the sender
}
