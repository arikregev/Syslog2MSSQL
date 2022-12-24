package Syslog;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class UDPServer {
    private final DatagramSocket ds;
    private volatile boolean running;
    private volatile boolean removeRunning;
    private byte[] buf;
    private final ConcurrentLinkedQueue<Message> list;
    private final ExecutorService executor;
    private final int numOfThreads = 8;

    public UDPServer(int port) throws IOException {
        this.ds = new DatagramSocket(port);
        this.buf = newByte();
        this.list = new ConcurrentLinkedQueue<>();
        this.executor = Executors.newFixedThreadPool(numOfThreads);
    }

    public void close() throws InterruptedException {
        this.running = false;
        System.out.println("Software shutdown process initiated.\nPlease wait while" +
                " writing queue to DB.");
        Thread.sleep(1000);
        while (list.size() > 0) {
            Thread.sleep(500);
            if(list.size() > 0)
                list.poll().run();
        }
        removeRunning = false;
        executor.shutdown();
    }

    public Thread RemoveThread() {
        return new Thread(() -> {
            while (removeRunning || (list.size() > 0)) {
                System.out.println("Queue Size: " + list.size());
                list.removeIf(x -> x.isCompleted());
                System.out.println("Queue Size: " + list.size());
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("Queue Size: " + list.size());
            }
        });
    }

    public Thread ReceiveMessagesThread(ConcurrentHashMap<String, String> map) {
        return new Thread(() -> {
            while (this.running) {
                DatagramPacket dp = new DatagramPacket(buf, buf.length);
                try {
                    ds.receive(dp);
                    list.add(new Message(
                            buf, map.getOrDefault(dp.getAddress().toString()
                            .replaceAll("/", ""), null), map));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                buf = newByte();
            }
        });
    }

    public Runnable run() {
        return () -> {
            while (running || list.size() > 0) {
                if (!list.isEmpty()) {
                    Message message = list.poll();
                    if (message != null)
                        new Thread(message).start();
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
    }

    public void listen(ConcurrentHashMap<String, String> map) throws InterruptedException {
        System.out.println("Syslog store in MSSQL DB, Written by Arik Regev");
        this.running = true;
        this.removeRunning = true;
        ReceiveMessagesThread(map).start();
        Thread.sleep(1000);
        for (int i = 0; i < numOfThreads; i++) {
            executor.execute(run());
            Thread.sleep(300);
        }
        RemoveThread().start();
    }

    public static byte[] newByte() {
        return new byte[65535];
    }
}