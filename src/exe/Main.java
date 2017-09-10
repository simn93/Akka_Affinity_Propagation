package exe;

/**
 * Compact Main
 *
 * @author Simone Schirinzi
 */
public class Main {
    public static void main(String args[]){
        if(args[0].equals("create")) Creator.main(java.util.Arrays.copyOfRange(args,1,args.length));
        if(args[0].equals("listen")) Listener.main(java.util.Arrays.copyOfRange(args,1,args.length));
    }
}