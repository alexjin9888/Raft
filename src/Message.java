import java.io.Serializable;

/*
 * This is an message that is sent and received among servers.
 */
public abstract class Message implements Serializable {
	// serverId is candidateId in RequestVoteRequest
	//             leaderId in AppendEntriesRequest
    String serverId;
    int term; // currentTerm of the sender
}
