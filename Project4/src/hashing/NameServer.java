package hashing;

import java.io.*;
import java.net.*;
import java.util.*;

public class NameServer {
    private int id;
    private int port;
    private String bootstrapIP;
    private int bootstrapPort;

    //local states
    private TreeMap<Integer,String> keyValueStore = new TreeMap<>();
    private int predecessor = -1;
    private int successor   = -1;

    public NameServer(int id, int port, String bootstrapIP, int bootstrapPort) {
        this.id = id;
        this.port = port;
        this.bootstrapIP = bootstrapIP;
        this.bootstrapPort = bootstrapPort;
    }

    public void start() {
        System.out.println("Name Server " + id + " started on port " + port);
        
        new Thread(this::listen).start();                   //begin listener thread for inter‑server messages

        Scanner sc = new Scanner(System.in);

        while (true) {
            System.out.print(">> ");
            String command = sc.nextLine().trim();
            if (command.equalsIgnoreCase("exit")) {
                sendCommandToBootstrap("exit " + id);
                System.out.println("Request sent to Bootstrap Server to exit.");
                break;
            } else if (command.equalsIgnoreCase("enter")) {
                sendCommandToBootstrap("enter " + id + " " + port);
                System.out.println("Request sent to Bootstrap Server to enter.");
            } else {
                System.out.println("Invalid command. Use 'enter' or 'exit'");
            }
        }
    }

    //helper to listern to network messages
    private void listen() {
        try (ServerSocket ss = new ServerSocket(port)) {
            while (true) {
                Socket s = ss.accept();
                new Thread(new clientHandler(s)).start();
            }
        } catch (IOException e) {
            System.out.println("Listener error: " + e.getMessage());
        }
    }

    class clientHandler implements Runnable {
        Socket socket;
        
        clientHandler(Socket s) {
            socket = s; 
        }

        public void run() {
            try (BufferedReader in = new BufferedReader(
                     new InputStreamReader(socket.getInputStream()));
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

                String line = in.readLine();

                if (line == null) {
                    return;
                }

                String[] p = line.trim().split(" ");
                
                switch (p[0].toLowerCase()) {
                    case "neighbors":            //from bootstrap on join
                        predecessor = Integer.parseInt(p[1]);
                        successor   = Integer.parseInt(p[4]);

                        System.out.println("Successful entry. Key range: [" +
                                          predecessor + ", " + id + "]");
                        System.out.println("Predecessor: " + predecessor +
                                           "  Successor: " + successor);
                        break;

                    case "setsucc":              // bootstrap to succ changed
                        successor = Integer.parseInt(p[1]);
                        System.out.println("Updated successor: " + successor);
                        break;

                    case "setpred":              // bootstrap to pred changed
                        predecessor = Integer.parseInt(p[1]);
                        System.out.println("Updated predecessor: " + predecessor);
                        break;

                    case "lookup":
                        int key = Integer.parseInt(p[1]);
                        if (keyValueStore.containsKey(key))
                            out.println("found " + keyValueStore.get(key));
                        else
                            out.println("notfound");
                        break;

                    case "insert":
                        keyValueStore.put(Integer.parseInt(p[1]), p[2]);
                        out.println("ok");
                        break;

                    case "delete":
                        out.println( keyValueStore.remove(Integer.parseInt(p[1])) != null ?
                                     "deleted" : "notfound");
                        break;

                    default:
                        out.println("err");
                }
            } catch (IOException e) {
                System.out.println("Client Handler Error: " + e.getMessage());
            }
        }
    }

    private void sendCommandToBootstrap(String message) {
        try (
            Socket socket = new Socket(bootstrapIP, bootstrapPort);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
        ) {
            out.println(message);
        } catch (IOException e) {
            System.out.println("Error connecting to Bootstrap Server: " + e.getMessage());
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.out.println("Usage: java NameServer <nsConfigFile>");
            return;
        }

        BufferedReader br = new BufferedReader(new FileReader(args[0]));

        int id = Integer.parseInt(br.readLine().trim());
        int port = Integer.parseInt(br.readLine().trim());
        
        String[] bootstrapInfo = br.readLine().trim().split(" ");
        String bootstrapIP = bootstrapInfo[0];
        int bootstrapPort = Integer.parseInt(bootstrapInfo[1]);
        br.close();

        NameServer server = new NameServer(id, port, bootstrapIP, bootstrapPort);
        server.start();
    }
}
