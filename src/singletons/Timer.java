package singletons;
import java.time.Instant;
import java.util.Date;

/**
 * Timer class that implements timeout logic for
 *   1) Election Timeout for Followers and Candidates
 *   2) Heartbeat Timeout for leaders
 *   See RAFT spec for more detailed descriptions
 */
public class Timer {
    private Date lastTimeoutTime;
    private int timeoutInterval;

    public Timer() {
        super();
        this.lastTimeoutTime = null;
        this.timeoutInterval = 0;
    }

    /**
     * @param timeoutInterval time we will wait until next timeout
     */
    public void reset(int timeoutInterval) {
        this.lastTimeoutTime = Date.from(Instant.now());
        this.timeoutInterval = timeoutInterval;
    }
    
    /** 
     * @return whether timeoutInterval has passed since lastTimeoutTime
     */
    public boolean timeIsUp() {
        if (lastTimeoutTime == null) {
            return true;
        }
        return Date.from(Instant.now()).getTime() - 
            lastTimeoutTime.getTime() >= timeoutInterval;
    }
}
