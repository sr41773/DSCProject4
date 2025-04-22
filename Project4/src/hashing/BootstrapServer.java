package hashing;

import java.io.*;
import java.net.*;
import java.util.*;

public class BootstrapServer {
    private int id;                     // usually 0
    private int port;
    private TreeMap<Integer, String> keyValueStore = new TreeMap<>();
    private TreeMap<Integer, ServerInfo> serverRing = new TreeMap<>();

    public BootstrapServer(int id, int port, String configFile) throws IOException {
        this.id = id;
        this.port = port;
        loadInitialData(configFile);
        serverRing.put(id, new ServerInfo("localhost", port)); // add self to ring
    }

    private void loadInitialData(String configFile) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(configFile));
        br.readLine(); // skip ID
        br.readLine(); // skip port
        String line;
        while ((line = br.readLine()) != null) {
            String[] parts = line.trim().split(" ");
            if (parts.length == 2) {
                int key = Integer.parseInt(parts[0]);
                keyValueStore.put(key, parts[1]);
            }
        }
        br.close();
    }

    public void start() throws IOException {
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("Bootstrap Server started on port " + port);
        new Thread(() -> userInteraction()).start();            //CLI thread
        while (true) {
            Socket client = serverSocket.accept();
            new Thread(new ServerHandler(client)).start();
        }
    }

    private void userInteraction() {
        Scanner sc = new Scanner(System.in);
        while (true) {
            System.out.print(">> ");
            String[] input = sc.nextLine().trim().split(" ");
            if (input.length == 0) continue;

            switch (input[0].toLowerCase()) {
                case "lookup":
                    if (input.length == 2) {
                        int key = Integer.parseInt(input[1]);
                        lookup(key);
                    } else System.out.println("Usage: lookup <key>");
                    break;
                case "insert":
                    if (input.length == 3) {
                        int key = Integer.parseInt(input[1]);
                        insert(key, input[2]);
                    } else System.out.println("Usage: insert <key> <value>");
                    break;
                case "delete":
                    if (input.length == 2) {
                        int key = Integer.parseInt(input[1]);
                        delete(key);
                    } else System.out.println("Usage: delete <key>");
                    break;
                default:
                    System.out.println("Unknown command");
            }
        }
    }

    private void lookup(int key) {
        int res = findSuccessor(key);
        List<Integer> path = clockwisePath(0, res);

        if (res == 0) {
            if (keyValueStore.containsKey(key)) {
                System.out.println("Key " + key + " found with value: " + keyValueStore.get(key));
                //System.out.println("Server path: [0]");               // for bootstrap server
            } else {
                System.out.println("Key not found.");
                //System.out.println("Server path: [0]");
            }
        } else {
            String response = remoteCommand(res, "lookup " + key).trim();
            if (response.startsWith("found")) {
                System.out.println("Key " + key + " found with value: " + response.substring(6));
            } else {
                System.out.println("Key not found.");
            }
        }
        System.out.println("Server path: " + path);

    }

    private void insert(int key, String value) {
        int res = findSuccessor(key);
        List<Integer> path = clockwisePath(0, res);

        if (res == 0) {
            keyValueStore.put(key, value);
            System.out.println("Inserted at Bootstrap Server");
        } else {
            remoteCommand(res, "inset " + key + " " + value);
            System.out.println("Inserted at Name Server " + res);

        }
        System.out.println("Server path: " + path);
    }

    private void delete(int key) {
        int res = findSuccessor(key);
        List<Integer> path = clockwisePath(0, res);
        String result;

        if (res == 0) {
            result = (keyValueStore.remove(key) != null) ? "Successful deletion" : "Key not found.";
            //System.out.println("Server path: [0]");
        } else {
            result = remoteCommand(res, "delete " + key).trim()
                     .equals("deleted") ? "Successful deletion" : "Key not found.";
            //System.out.println("Key not found.");
            //System.out.println("Server path: [0]");
        }
        System.out.println(result);
        System.out.println("Server path: " + path);
    }

    //clockwise successor of id
    private int findSuccessor(int key) {
        Integer candidate = serverRing.ceilingKey(key);
        return (candidate != null) ? candidate : serverRing.firstKey();
    }


    //building list of id's
    private List<Integer> clockwisePath(int from, int to) {
        List<Integer> path = new ArrayList<>();
        if (from == to) {
            path.add(from);
            return path;
        }

        SortedMap<Integer, ServerInfo> tail = serverRing.tailMap(from, false);
        tail.keySet().forEach(id -> { 
            if (id <= to) {
                path.add(id); 
            }
        });

        serverRing.headMap(from, false).keySet().forEach(path::add);
        if (!path.contains(to)) {
            path.add(to);
        }

        path.add(0, from);
        return path;
    }

    //tcp message
    private String remoteCommand(int id, String message) {
        ServerInfo info = serverRing.get(id);

        if (info == null) {
            return "";
        }

        try (Socket s = new Socket(info.ip, info.port);
             BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
             PrintWriter out = new PrintWriter(s.getOutputStream(), true)) {
            out.println(message);
            return in.readLine();
        } catch (IOException e) {
            return "";
        }
    }

    class ServerHandler implements Runnable {
        private Socket socket;

        ServerHandler(Socket socket) {
            this.socket = socket;
        }

        public void run() {
            try (
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
            ) {
                String input = in.readLine();
                if (input == null || input.isEmpty()) return;
                String[] parts = input.trim().split(" ");
                String command = parts[0];

                if ("enter".equalsIgnoreCase(command)) {
                    int serverId = Integer.parseInt(parts[1]);
                    int serverPort = Integer.parseInt(parts[2]);

                    String newIp = socket.getInetAddress().getHostAddress();
                    serverRing.put(serverId, new ServerInfo(newIp, serverPort));

                    //serverRing.put(serverId, new ServerInfo(socket.getInetAddress().getHostAddress(), serverPort));

                    // update successor and predecessor
                    int succId = serverRing.higherKey(serverId) != null ?
                                  serverRing.higherKey(serverId) :
                                  serverRing.firstKey();

                    int predId = serverRing.lowerKey(serverId) != null ?
                                  serverRing.lowerKey(serverId) :
                                  serverRing.lastKey();
                    
                    // send neighbour info back to entering node
                    out.println("neighbors " + predId + " " + serverRing.get(predId).ip + " " + serverRing.get(predId).port + " " 
                                            + succId + " " + serverRing.get(succId).ip + " " + serverRing.get(succId).port);

                    migrateKeysToNewNode(predId, serverId, serverId);    //move keys to new node

                    System.out.println("Server " + serverId + " has entered the ring.");
                    System.out.println("Current ring: " + serverRing.keySet());

                    notifyNeighbor(predId, serverId, true);   // update successor
                    notifyNeighbor(succId, serverId, false);  // update predecessor

                } else if ("exit".equalsIgnoreCase(command)) {
                    //int serverId = Integer.parseInt(parts[1]);
                    int leavingId = Integer.parseInt(parts[1]);

                    // update successor and predecessor
                    int succId = serverRing.higherKey(leavingId) != null ?
                                  serverRing.higherKey(leavingId) :
                                  serverRing.firstKey();
                    int predId = serverRing.lowerKey(leavingId) != null ?
                                  serverRing.lowerKey(leavingId) :
                                  serverRing.lastKey();
                    
                    ServerInfo succInfo = serverRing.get(succId);
                    ServerInfo predInfo = serverRing.get(predId);
                    // send neighbour info back to leaving node

                    try (Socket s = new Socket(
                            socket.getInetAddress().getHostAddress(), // back to leaver
                            Integer.parseInt(parts.length == 3 ? parts[2] : "0"))) {

                    } catch (Exception ignore) {
                        //ignore
                    }

                    serverRing.remove(leavingId);
                    
                    notifyNeighbor(predId, succId, true);   // new succ
                    notifyNeighbor(succId, predId, false);  // new pred

                    System.out.println("Server " + leavingId + " has exited the ring.");
                    System.out.println("Current ring: " + serverRing.keySet());

                } else {
                    System.out.println("Unknown message: " + input);
                }
            } catch (IOException e) {
                System.out.println("Error in ServerHandler: " + e.getMessage());
            }
        }


        // helper to let a neighbour know about new succ/pred
        private void notifyNeighbor(int targetId, int newNeighbor, boolean isSucc) {
            if (targetId == 0)
                return; // Bootstrap keeps full map anyway
            
            ServerInfo t = serverRing.get(targetId);
            
            if (t == null) 
                return;

            try (Socket s = new Socket(t.ip, t.port);
                     PrintWriter out = new PrintWriter(s.getOutputStream(), true)) {
                // Send the message to the target server
                out.println((isSucc ? "setsucc" : "setpred") + " " + newNeighbor);

            } catch (IOException ignore) {
                //ignore
            }
        }

        //move keys (predId, serverId) from Bootstrap store → new node
        private void migrateKeysToNewNode(int predId, int newId, int targetId) {
            ServerInfo target = serverRing.get(targetId);
            
            if (target == null) 
                return;

            List<Integer> toMove = new ArrayList<>();
            
            keyValueStore.keySet().forEach(k -> {
                if (inRange(predId, newId, k)) toMove.add(k);
            });

            for (int k : toMove) {
                String v = keyValueStore.remove(k);
                remoteCommand(targetId, "insert " + k + " " + v);
            }
        }

        //check if key is in range of predId and serverId
        private boolean inRange(int pred, int curr, int key) {
            if (pred < curr) return key > pred && key <= curr;
            // wrap‑around
            return key > pred || key <= curr;
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.out.println("Usage: java BootstrapServer <bnConfigFile>");
            return;
        }

        BufferedReader br = new BufferedReader(new FileReader(args[0]));
        int id = Integer.parseInt(br.readLine().trim());
        int port = Integer.parseInt(br.readLine().trim());
        br.close();

        BootstrapServer server = new BootstrapServer(id, port, args[0]);
        server.start();
    }
}

class ServerInfo {
    String ip;
    int port;

    ServerInfo(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public String toString() {
        return ip + ":" + port;
    }
}
