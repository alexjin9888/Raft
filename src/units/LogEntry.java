package units;

import java.io.Serializable;

/**
 * A log entry in a server's command log.
 */
public class LogEntry implements Serializable {
    public int index; // index of log entry in the log
    public int term; // term of the leader when it sent this log
    public String command; // To be executed on each server

    /**
     * @param index   see above
     * @param command see above
     * @param term    see above
     */
    public LogEntry(int index, int term, String command) {
        this.index = index;
        this.term = term;
        this.command = command;
    }
    
    public LogEntry(String stringifiedLogEntry) {
        
    }
    
    @Override
    public String toString() {
        return "LogEntry [index=" + index + ", term=" + term + ", command="
                + command + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        
        LogEntry other = (LogEntry) obj;
        return this.index == other.index && this.term == other.term;
    }
    
}
