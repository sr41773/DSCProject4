import java.io.*;
import java.net.*;
import java.util.*;

public class BootstrapServer {
    private int id;
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
        new Thread(() -> userInteraction()).start();
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
        if (keyValueStore.containsKey(key)) {
            System.out.println("Key " + key + " found with value: " + keyValueStore.get(key));
            System.out.println("Server path: [0]");
        } else {
            System.out.println("Key not found.");
            System.out.println("Server path: [0]");
        }
    }

    private void insert(int key, String value) {
        keyValueStore.put(key, value);
        System.out.println("Inserted at Bootstrap Server");
        System.out.println("Server path: [0]");
    }

    private void delete(int key) {
        if (keyValueStore.containsKey(key)) {
            keyValueStore.remove(key);
            System.out.println("Successful deletion");
            System.out.println("Server path: [0]");
        } else {
            System.out.println("Key not found.");
            System.out.println("Server path: [0]");
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
                    serverRing.put(serverId, new ServerInfo(socket.getInetAddress().getHostAddress(), serverPort));
                    System.out.println("Server " + serverId + " has entered the ring.");
                    System.out.println("Current ring: " + serverRing.keySet());
                } else if ("exit".equalsIgnoreCase(command)) {
                    int serverId = Integer.parseInt(parts[1]);
                    serverRing.remove(serverId);
                    System.out.println("Server " + serverId + " has exited the ring.");
                    System.out.println("Current ring: " + serverRing.keySet());
                } else {
                    System.out.println("Unknown message: " + input);
                }
            } catch (IOException e) {
                System.out.println("Error in ServerHandler: " + e.getMessage());
            }
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
