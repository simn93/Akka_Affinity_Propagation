package affinityPropagation;

import akka.actor.AbstractActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;

/**
 * Actor for logging
 *
 * Simple print all String received
 * @author Simone Schirinzi
 */
public class LogActor extends AbstractActor {
    /**
     * Akka log
     */
    private final LoggingAdapter log;

    public LogActor(){
        log = Logging.getLogger(getContext().getSystem(), this);
    }

    /**
     * Receive String and log them
     *
     * @return Handle for messages
     */
    @Override public Receive createReceive() {
        return receiveBuilder().match(String.class, log::info).build();
    }
}
