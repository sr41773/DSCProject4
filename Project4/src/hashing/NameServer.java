package hashing;

import java.io.*;
import java.net.*;
import java.util.*;

public class NameServer {
    private int id;
    private int port;
    private String bootstrapIP;
    private int bootstrapPort;

    public NameServer(int id, int port, String bootstrapIP, int bootstrapPort) {
        this.id = id;
        this.port = port;
        this.bootstrapIP = bootstrapIP;
        this.bootstrapPort = bootstrapPort;
    }

    public void start() {
        System.out.println("Name Server " + id + " started on port " + port);
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
