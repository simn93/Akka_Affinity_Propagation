package exe;

import affinityPropagation.*;
import akka.actor.*;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.json.simple.*;
import org.json.simple.parser.*;
import java.io.FileReader;
import java.io.IOException;

/**
 * Creator class
 * first Load file
 * then Start system
 *
 * @author Simone Schirinzi
 */
public class Creator {
    private static Address[] remoteAddress;
    private static String lineMatrix;
    private static String colMatrix;
    private static String lineFormat;
    private static int size;
    private static int subClusterSize;
    private static double lambda;
    private static long enoughIterations;
    private static int sendEach;
    private static double sigma;
    private static boolean verbose;

    public static void main(String[] args) {
        startSystem(args);
    }

    private static void startSystem(String[] args) {
        loadSetting("./setting.json");
        verbose = false;

        String localIP = "127.0.0.1", bindIP = "127.0.0.1";
        int localPort = 2552, bindPort = 2552;
        String listenerConfig;
        boolean nat = false;

        for(int i = 0; i < args.length; i+=2) switch (args[i]) {
            case "-sett":
                loadSetting(args[i + 1]);
                break;
            case "-line":
                lineMatrix = args[i + 1];
                break;
            case "-col":
                colMatrix = args[i + 1];
                break;
            case "-format":
                lineFormat = args[i + 1];
                break;
            case "-size":
                size = Integer.parseInt(args[i + 1]);
                break;
            case "-cluster":
                subClusterSize = Integer.parseInt(args[i + 1]);
                break;
            case "-lambda":
                lambda = Double.parseDouble(args[i + 1]);
                break;
            case "-enough":
                enoughIterations = Long.parseLong(args[i + 1]);
                break;
            case "-each":
                sendEach = Integer.parseInt(args[i + 1]);
                break;
            case "-sigma":
                sigma = Double.parseDouble(args[i + 1]);
                break;
            case "-verbose":
                verbose = Boolean.parseBoolean(args[i+1]);
                break;
            case "-ip":
                localIP = args[i+1];
                break;
            case "-bind":
                bindIP = args[i+1];
                nat = true;
                break;
            case "-port":
                localPort = bindPort = Integer.parseInt(args[i+1]);
                break;
            default:
                System.out.println(args[i] + " not recognized");
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
                .withFallback(ConfigFactory.load("common"));

        ActorSystem system = ActorSystem.create("creatorSystem", config);

        new AffinityPropagation(
                lineMatrix,colMatrix,
                lineFormat,size,subClusterSize,
                system,
                system.actorOf(Props.create(LogActor.class),"log"),
                remoteAddress,
                verbose,lambda,enoughIterations,sendEach,sigma
        );
    }

    private static void loadSetting(String sett_path){
        JSONObject jsonObject;
        try(FileReader r = new FileReader(sett_path)){
            JSONParser parser = new JSONParser();
            jsonObject = (JSONObject) parser.parse(r);

            JSONArray array = (JSONArray) jsonObject.get("deploy");
            remoteAddress = new Address[array.size()];

            int i = 0; for(Object o: array) {
                remoteAddress[i] = new Address(
                                "akka.tcp",
                                "lookupSystem",
                                ((String) ((JSONObject) o).get("ip")),
                                ((Long) ((JSONObject) o).get("port")).intValue()
                        );
                i++;
            }

            lineMatrix = ((String) jsonObject.get("lineMatrix"));
            colMatrix = ((String) jsonObject.get("colMatrix"));
            lineFormat = ((String) jsonObject.get("lineFormat"));
            size = ((Long) jsonObject.get("size")).intValue();
            subClusterSize = ((Long) jsonObject.get("subClusterSize")).intValue();
            lambda = ((Double) jsonObject.get("lambda"));
            enoughIterations = ((Long) jsonObject.get("enoughIterations"));
            sendEach = ((Long) jsonObject.get("sendEach")).intValue();
            sigma = ((Double) jsonObject.get("sigma"));
            verbose = ((Boolean) jsonObject.get("verbose"));
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
    }}