package affinityPropagation;
import akka.actor.AbstractActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public class LogActor extends AbstractActor {
    private LoggingAdapter log;

    public LogActor(){
        log = Logging.getLogger(getContext().getSystem(), this);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder().match(String.class, s -> log.info(s)).build();
    }
}
