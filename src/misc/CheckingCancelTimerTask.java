package misc;

import java.util.TimerTask;

/**
 * A TimerTask subclass that allows you to check whether a TimerTask has
 * been cancelled.
 */
public abstract class CheckingCancelTimerTask extends TimerTask {
    public boolean isCancelled;
    
    public CheckingCancelTimerTask() {
        super();
        isCancelled = false;
    }
    
    public boolean cancel() {
        isCancelled = true;
        return super.cancel();
    }
    
}
