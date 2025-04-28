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

    private String successorIP = null;
    private int    successorPort = -1;

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
                /**if (successor != -1 && successor != id) {
                    for (var e : keyValueStore.entrySet()) {
                        remoteInsertToSuccessor(e.getKey(), e.getValue());
                    }
                    keyValueStore.clear();
                }
                
                sendCommandToBootstrap("exit " + id);
                System.out.println("Request sent to Bootstrap Server to exit.");
                break;*/
                gracefulExit();
                return;

            } else if (command.equalsIgnoreCase("enter")) {
                //sendCommandToBootstrap("enter " + id + " " + port);
                //System.out.println("Request sent to Bootstrap Server to enter.");
                performEntry();
                //break;
                continue;

            } else {
                System.out.println("Invalid command. Use 'enter' or 'exit'");
            }
        }
    }

    //helper to perform entry into the ring
    private void performEntry() {
        try (Socket s = new Socket(bootstrapIP, bootstrapPort);
             BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
             PrintWriter out = new PrintWriter(s.getOutputStream(), true)) {
    
            out.println("enter " + id + " " + port);
    
            String line = in.readLine();                  // read from bootstrap
            if (line == null || !line.startsWith("neighbors")) {
                System.out.println("Bootstrap did not reply — entry failed");
                return;
            }
            String[] p = line.trim().split(" ");
    
            predecessor   = Integer.parseInt(p[1]);
            successor     = Integer.parseInt(p[4]);
            successorIP   = p[5];
            successorPort = Integer.parseInt(p[6]);
    
            System.out.println("Successful entry. Key range: (" + predecessor + ", " + id + "]");
            System.out.println("Predecessor: " + predecessor + "  Successor: "   + successor);
    
            requestKeysFromSuccessor();            // pull KV pairs

        } catch (IOException e) {
            System.out.println("Entry error: " + e.getMessage());
        }
    }
    
    // moves keys to successor and informs bootstrap
    private void gracefulExit() {
        if (successor != -1 && successor != id) {
            for (var e : keyValueStore.entrySet())
                remoteInsertToSuccessor(e.getKey(), e.getValue());
            keyValueStore.clear();
        }
        try (Socket s = new Socket(bootstrapIP, bootstrapPort);
             PrintWriter out = new PrintWriter(s.getOutputStream(), true)) {
            out.println("exit " + id);
        } catch (IOException ignore) { }
        System.out.println("Request sent to Bootstrap Server to exit.");
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
                        successorIP  = p[5];
                        successorPort= Integer.parseInt(p[6]); 

                        System.out.println("Successful entry. Key range: [" +
                                          predecessor + ", " + id + "]");
                        System.out.println("Predecessor: " + predecessor +
                                           "  Successor: " + successor);

                        requestKeysFromSuccessor(); // pull keys from successor

                        break;

                    case "setsucc":              // bootstrap to succ changed
                        successor = Integer.parseInt(p[1]);
                        successorIP   = socket.getInetAddress().getHostAddress();
                        successorPort = Integer.parseInt(p[2]);      // bootstrap passes port in p[2]

                        System.out.println("Updated successor: " + successor);
                        break;

                    case "setpred":              // bootstrap to pred changed
                        predecessor = Integer.parseInt(p[1]);
                        System.out.println("Updated predecessor: " + predecessor);
                        break;

                    case "transfer":
                        int fromKey = Integer.parseInt(p[1]);
                        int toKey   = Integer.parseInt(p[2]);
                        int count   = Integer.parseInt(p[3]);

                        for (int i = 0; i < count; i++) {
                            String kv = in.readLine();
                            String[] kvp = kv.split(" ", 2);
                            keyValueStore.put(Integer.parseInt(kvp[0]), kvp[1]);
                        }

                        out.println("ack");
                        break;
                    
                    case "getkeys":        // requester: predID myID
                        int from = Integer.parseInt(p[1]);
                        int to   = Integer.parseInt(p[2]);
                    
                        // collect all keys in [from, to]
                        List<Integer> list = new ArrayList<>();
                        keyValueStore.forEach((k,v) -> {
                            if (inRange(from, to, k)) list.add(k);
                        });
                    
                        out.println("count " + list.size());
                    
                        for (int k : list) {
                            out.println(k + " " + keyValueStore.get(k));
                            keyValueStore.remove(k);           // remove from my store
                        }
                        return;
                    
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

    private boolean inRange(int pred, int curr, int key) {
        if (pred < curr) return key > pred && key <= curr;
        // wrap‑around
        return key > pred || key <= curr;
    }

    //helper to ask successor to send keys
    private void requestKeysFromSuccessor() {
        if (successor == id) {
            return;                    // only node in ring
        }

        try (Socket s = new Socket(successorIP, successorPort);
             BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
             PrintWriter out = new PrintWriter(s.getOutputStream(), true)) {

            out.println("getkeys " + predecessor + " " + id);   // request
            String first = in.readLine();                        // count line from succ

            if (first == null || !first.startsWith("count")) 
                return;
            
            int n = Integer.parseInt(first.split(" ")[1]);

            for (int i = 0; i < n; i++) {
                String kv = in.readLine();
                String[] kvp = kv.split(" ", 2);

                keyValueStore.put(Integer.parseInt(kvp[0]), kvp[1]);
            }
        } catch (IOException ignore) {
            //Ignore
        }
    }

    private void remoteInsertToSuccessor(int k, String v) {
        try (Socket s = new Socket(successorIP, successorPort);
             PrintWriter out = new PrintWriter(s.getOutputStream(), true)) {
            out.println("insert " + k + " " + v);
        } catch (IOException ignore) {
            //ignore
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
