package Syslog;

import java.io.*;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

public class ReadConf {
    private File file;

    public ReadConf(String path) throws FileNotFoundException {
        file = new File(path);
    }

    public ConcurrentHashMap<String,String> readFile() throws IOException {
        ConcurrentHashMap<String,String> map = new ConcurrentHashMap<>();
        BufferedReader in = new BufferedReader(new FileReader(this.file));
        String line;
        while((line = in.readLine()) != null){
            if(line.charAt(0) == '#') continue;
            String[] confLine = line.split("=");
            map.put(confLine[0],confLine[1]);
        }
        return map;
    }
}
