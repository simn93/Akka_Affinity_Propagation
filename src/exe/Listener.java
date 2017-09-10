package exe;

import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Daemon starter
 *
 * @author Simone Schirinzi
 */
public class Listener {
    public static void main(String[] args){
        String localIP = "127.0.0.1", bindIP = "127.0.0.1";
        int localPort = 2553, bindPort = 2553;
        String listenerConfig;
        boolean nat = false;

        if(args != null){
            for(int i = 0; i < args.length; i++){
                switch (args[i]){
                    case "-ip" :
                        localIP = args[i+1];
                        localPort = Integer.parseInt(args[i+2]);
                        i+=2;
                        break;
                    case "-bind" :
                        nat = true;
                        bindIP = args[i+1];
                        bindPort = Integer.parseInt(args[i+2]);
                        i+=2;
                        break;
                    default:
                        System.out.println("''" + args[i] + "'' non recognized command.");
                        break;
                }
            }
        }

        if(nat){
            listenerConfig =
                    "akka.remote.netty.tcp {\n" +
                            "  hostname = \"" + bindIP +"\"\n" +
                            "  port = " + bindPort + "\n" +
                            "  bind-hostname = " + localIP + "\n" +
                            "  bind-port = " + localPort + "\n" +
                            "}";
        } else {
            listenerConfig =
                    "akka.remote.netty.tcp {\n" +
                            "  hostname = \"" + localIP +"\"\n" +
                            "  port = " + localPort + "\n" +
                            "}";
        }



        Config config = ConfigFactory.parseString(listenerConfig)
                .withFallback(ConfigFactory.load("common.conf"));

        ActorSystem.create("lookupSystem", config);
    }
}
