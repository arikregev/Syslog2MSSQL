package Syslog;

import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MSSQL {

    private Connection con;
    private final String serverPath;

    public MSSQL(ConcurrentHashMap<String, String> conf) throws Exception {
        serverPath = build(conf).toString();
        con = DriverManager.getConnection(build(conf).toString());
    }

    public void checkConnection() throws SQLException {
        if (con.isClosed())
            con = java.sql.DriverManager.getConnection(this.serverPath);
    }

    public void close() throws SQLException {
        con.close();
    }

    private boolean checkParameters(Map<String, String> map) {
        String[] params = {"serverAddress", "port", "databaseName", "user", "password"};
        for (String s : params) {
            if (!map.containsKey(s)) return false;
        }
        return true;
    }

    private StringBuilder build(Map<String, String> map) {
        StringBuilder sb = new StringBuilder();
        sb.append("jdbc:sqlserver://");
        sb.append(map.get("serverAddress"));
        sb.append(":");
        sb.append(map.get("port"));
        addSemiColon(sb);
        sb.append("databaseName=");
        sb.append(map.get("databaseName"));
        addSemiColon(sb);
        sb.append("user=");
        sb.append(map.get("user"));
        addSemiColon(sb);
        sb.append("password=");
        sb.append(map.get("password"));
        return sb;
    }

    private void addSemiColon(StringBuilder sb) {
        sb.append(";");
    }

    //Database Actions
    private String getDBName(String tableName) {
        LocalDate date = LocalDate.now();
        return LocalDate.parse(date.format(DateTimeFormatter.ISO_LOCAL_DATE), DateTimeFormatter.ISO_LOCAL_DATE).toString() + " " + tableName;
    }

    private boolean CheckTableExist(String productName, Map<String, String> map) throws SQLException {
        DatabaseMetaData dmd = con.getMetaData();
        ResultSet rs = dmd.getTables(null, null, "%", null);
        String table = getDBName(productName);
        while (rs.next()) {
            if (rs.getString("TABLE_NAME").contains(table)) {
                return true;
            }
        }
        return false;
    }

    private synchronized HashSet<String> GetTableCurrentFields(String productName) {
        HashSet<String> tableColumns = new HashSet<>();
        DatabaseMetaData dmd;
        try {
            dmd = con.getMetaData();
            ResultSet rs = dmd.getColumns(null, null, getDBName(productName), null);
            while (rs.next()) {
                tableColumns.add(rs.getString("COLUMN_NAME"));
            }
            return tableColumns;
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            return null;
        }
    }

    private void CheckAndAlterTable(String productName, Map<String, String> map, HashSet<String> tableColumns) {
        //synchronized (this) {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (!tableColumns.contains(entry.getKey())) {
                String sb = "ALTER TABLE [" + getDBName(productName) + "] ADD [" + entry.getKey() + "] VARCHAR(8000);";
                try {
                    checkConnection();
                } catch (SQLException e) {
                }
                tableColumns = GetTableCurrentFields(productName); //check if another instance already add the column.
                try {
                    if (!tableColumns.contains(entry.getKey())) {
                        this.con.createStatement().execute(sb);
                    }

                } catch (SQLException throwables) {
                    System.out.println("######### " + sb + "############");
                    throwables.printStackTrace();
                }
            }
        }
    }
    //}

    private HashMap<String, String> SyslogToMap(String syslog) {
        HashMap<String, String> map = new HashMap<>();
        String[] syslogParams = (syslog.split(">"))[1].split(" ");
        map.put("syslogCode", syslog.split(">")[0].replaceAll("<", "").replaceAll(">", ""));
        for (String s : syslogParams) {
            String[] param = s.split("=");
            if (param.length > 1)
                map.put(param[0], param[1].replaceAll("'", "\'"));
            else {
                map.put("syslog", syslog.replaceAll("'", "\'"));
                break; //If there is a syslog input to SQL error, the entire syslog will be written to syslog column.
            } //If no split available
        }
        return map;
    }

    public synchronized void WriteToDB(String str) {
        try {
            //checkConnection();
            this.con.createStatement().execute(str);
            this.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public void PrepareForWriteToDB(String productName, String syslog) {
        HashMap<String, String> map = SyslogToMap(syslog);
        try {
            if (!CheckTableExist(productName, map)) {
                //synchronized (this) {
                if (!CheckTableExist(productName, map))
                    this.WriteToDB(SyslogTableStatementBuilder(productName, map));
                //}
            }
            HashSet<String> columns = GetTableCurrentFields(productName);
            if (columns != null) CheckAndAlterTable(productName, map, columns);
            WriteToDB(SyslogStatementBuilder(productName, map).replaceAll("\"", ""));
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            WriteToDBAfterError(productName, syslog);
        }
    }

    public void WriteToDBAfterError(String productName, String syslog) {
        HashMap<String, String> map = new HashMap<>();
        map.put("syslog", syslog);
        GetTableCurrentFields(productName);
        WriteToDB(SyslogStatementBuilder(productName, map).replaceAll("\"", ""));
    }

    private String SyslogStatementBuilder(String productName, Map<String, String> map) {
        StringBuilder sb = new StringBuilder();
        StringBuilder sb2 = new StringBuilder();
        sb.append("INSERT INTO [").append(getDBName(productName)).append("] ("); //ToDo: Complete statement after table builder.
        sb2.append(" VALUES (");
        for (Map.Entry<String, String> entry : map.entrySet()) {
            sb.append("[").append(entry.getKey()).append("], ");
            sb2.append("'").append(entry.getValue().replaceAll("'", "")).append("'").append(", ");
        }
        sb.delete(sb.length() - 2, sb.length() - 1).append(")");
        sb2.delete(sb2.length() - 2, sb2.length() - 1).append(");");
        return sb.append(sb2.toString()).toString();
    }

    private String SyslogTableStatementBuilder(String table, Map<String, String> map) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE [").append(getDBName(table))
                .append("] (id int NOT NULL IDENTITY(1,1) PRIMARY KEY,");
        for (Map.Entry<String, String> entry : map.entrySet()) {
            sb.append(entry.getKey()).append(" VARCHAR(8000),");
        }
        sb.deleteCharAt(sb.length() - 1).append(");");
        return sb.toString();
    }
}
