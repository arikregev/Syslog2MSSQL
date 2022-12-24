package Syslog;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Close {

    public Close(UDPServer server) {
        new Thread(() -> {
            ServerSocket socket;
            Socket close;
            try {
                socket = new ServerSocket(3000);
                close = socket.accept();
                if (close != null) close.close();
                if (!socket.isClosed()) socket.close();
                server.close();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
    }
}