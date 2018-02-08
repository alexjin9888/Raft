package units;

/**
 * A log entry in a server's command log.
 * Proj2: figure out what to do with command string
 */
public class LogEntry {
    public String command; // To be executed on each server
    public int term; // term of the leader when it sent this log

    /**
     * @param command see above
     * @param term    see above
     */
    public LogEntry(String command, int term) {
        super();
        this.command = command;
        this.term = term;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        LogEntry other = (LogEntry) obj;
        
        if (command == null && other.command != null) {
            return false;
        } else if (!command.equals(other.command))
            return false;
        
        if (term != other.term)
            return false;
        
        return true;
    }
    
    @Override
    public String toString() {
        return "LogEntry [command=" + command + ", term=" + term + "]";
    }
}
