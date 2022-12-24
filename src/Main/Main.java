package Main;

import Syslog.Close;
import Syslog.ReadConf;
import Syslog.UDPServer;

import java.util.concurrent.ConcurrentHashMap;

public class Main {
    public static void main(String[] args) {
        if(args.length != 1) {
            System.out.println("Argument Error: please pass a valid path to config file!");
            return;
        }
        UDPServer udp;
        ConcurrentHashMap<String, String> map;
        try {
            map = new ReadConf(args[0]).readFile();
            udp = new UDPServer(514);
            udp.listen(map);
            new Close(udp);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
