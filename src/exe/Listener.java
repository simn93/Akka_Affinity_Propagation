package exe;

import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class Listener {
    public static void main(String[] args){
        String IP = "127.0.0.1";
        int port = 2553;
        if(args.length > 0)IP = args[0];

        String listenerConfig =
        "akka.remote.netty.tcp {\n" +
        "  hostname = \"" + IP +"\"\n" +
        "  port = " + port + "\n" +
        "}";

        Config config = ConfigFactory.parseString(listenerConfig)
                .withFallback(ConfigFactory.load("common.conf"));

        ActorSystem.create("lookupSystem", config);
    }
}
