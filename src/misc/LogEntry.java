package misc;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A log entry in a server's command log.
 */
public class LogEntry implements Serializable {
    public int index; // index of log entry in the log
    public int term; // term of the leader when it sent this log
    public String command; // To be executed on each server
    
    private final static Pattern pattern = Pattern.compile(
            "LogEntry \\[index=(\\d+), term=(\\d+), command=(.*)\\]");

    private static int INDEX_GRP = 1;
    private static int TERM_GRP = 2;
    private static int COMMAND_GRP = 3;
    /**
     * @param index   see top of class file
     * @param command see top of class file
     * @param term    see top of class file
     */
    public LogEntry(int index, int term, String command) {
        this.index = index;
        this.term = term;
        this.command = command;
    }
    
    public LogEntry(String stringifiedLogEntry) {
        Matcher m = pattern.matcher(stringifiedLogEntry);
        if (!m.matches()) {
            throw new PersistentStateException(
                    "Cannot parse log entry: " + stringifiedLogEntry);
        }
        this.index = Integer.parseInt(m.group(INDEX_GRP));
        this.term = Integer.parseInt(m.group(TERM_GRP));
        this.command = m.group(COMMAND_GRP);
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
