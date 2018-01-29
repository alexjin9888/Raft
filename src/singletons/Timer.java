package singletons;
import java.time.Instant;
import java.util.Date;

/**
 * Timer class that implements timeout logic for
 *   1) Election Timeout for Followers and Candidates
 *   2) Heartbeat Timeout for leaders
 *   See RAFT spec for mroe detailed descriptions
 *
 */
public class Timer {
    /**
     * lastTimeoutTime
     * timeoutInterval different for Election Timeout and Heartbeat Timeout
     */
    private Date lastTimeoutTime;
    private int timeoutInterval;

    /**
     * Constructor
     */
    public Timer() {
        super();
        this.lastTimeoutTime = null;
        this.timeoutInterval = 0;
    }

    /**
     * @param timeoutInterval Either Election Timeout or Heartbeat Timeout
     */
    public void reset(int timeoutInterval) {
        this.lastTimeoutTime = Date.from(Instant.now());
        this.timeoutInterval = timeoutInterval;
    }
    
    /**
     * Checks if we need to perform an election or a heartbeat message
     */
    public boolean timeIsUp() {
        if (lastTimeoutTime == null) {
            return true;
        }
        return Date.from(Instant.now()).getTime() - 
            lastTimeoutTime.getTime() >= timeoutInterval;
    }
}
