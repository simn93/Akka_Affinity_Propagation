import akka.actor.ActorSystem;
import com.typesafe.config.ConfigFactory;

public class Listener {
    public static void main(String[] args){
        if(args.length == 0)args = new String[]{"listener"};
        ActorSystem.create("lookupSystem", ConfigFactory.load(args[0]));
    }
}
