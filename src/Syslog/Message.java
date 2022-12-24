package Syslog;

import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;

public class Message implements Runnable{
    private final byte[] message;
    private final String sysName;
    private final MSSQL sql;
    private volatile boolean completed;

    public Message(byte[] message, String sysName, ConcurrentHashMap<String, String> conf) throws Exception {
        this.message = message;
        this.sysName = sysName;
        this.sql = new MSSQL(conf);
        this.completed = false;
    }
    @Override
    public void run() {
        String s = data(this.message).toString();
        this.sql.PrepareForWriteToDB(this.sysName, s);
        try {
            sql.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        this.completed = true;
    }

    public boolean isCompleted() { return completed; }

    private StringBuilder data(byte[] a) {
        if (a == null) return null;
        StringBuilder ret = new StringBuilder();
        int i = 0;
        while (a[i] != 0) {
            char ch = (char) a[i];
            if(ch !='"' || ch != '\'') {
                ret.append((char) a[i]);
            }
            i++;
        }
        return ret;
    }
}