/* 
 * A class that manages the persistent state of a server  
 */
public class PersistentState {
    private String myId; // Unique identification (Id) per server
    // Persistent State
    // * Latest term server has seen (initialized to 0 on first boot, increases
    //   monotonically)
    private int currentTerm;
    // * candidateId that received vote in current term (or null if none)
    private String votedFor;
    // * log entries; each entry contains command for state machine, and term
    //   when entry was received by leader (first index is 0)
    private List<LogEntry> log;

    // Class constructor
    // Creates an object that manages persistent state for server with ID=myId
 	// During instantiation, it will do 1 of 2 things:
 	//   1) If persistent state file exists, load it
 	//   2) Otherwise initialize persistent state variables
	public PersistentState(String myId) {
    }

    // Check to see if there is a file on disk corresponding to the server's
    // persistent state
    public boolean checkStorageExists() {

	}

	// Load from file the persistent state of this server
	//  * invariant: the file with persistent state exists
	public void load() {

	}

	// Writes to file the current persistent state of this server
	public void save() {

	}
}
