package edu.oswego.raftchat;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Parameter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

public class RaftNode implements Runnable {
    private int currentTerm = 0;
    private InetSocketAddress votedFor;
    private List<LogEntry> log = Collections.synchronizedList(new ArrayList<>());
    private State state = State.FOLLOWER;
    private int commitIndex = -1;
    private int lastApplied = -1;
    private ServerSocket serverSocket;
    private Set<Socket> clients = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private Set<Socket> peers = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private InetSocketAddress leaderId;

    // leader state
    Map<InetSocketAddress, Integer> nextIndex = new ConcurrentHashMap<>();
    Map<InetSocketAddress, Integer> matchIndex = new ConcurrentHashMap<>();

    // queue to send to client
    BlockingQueue<ChatMessage> clientQueue;

    enum State {
        LEADER, CANDIDATE, FOLLOWER
    }

    public RaftNode(BlockingQueue clientQueue) throws IOException {
        this(clientQueue, 0);
    }

    public RaftNode(BlockingQueue clientQueue, int port) throws IOException {
        this.clientQueue = clientQueue;
        serverSocket = new ServerSocket(port);
    }

    @Override
    public void run() {
        while (true) {
            switch (state) {
                case FOLLOWER: {
                }
                case CANDIDATE: {
                }
                case LEADER: {
                }
            }
        }
    }

    private void listen() {
        while(true) {
            try (Socket newConnection = serverSocket.accept()) {
                new Thread(new Runnable() {
                    Socket client = newConnection;

                    @Override
                    public void run() {
                        byte[] buffer = new byte[8192];
                        RaftMessageBuffer messageBuffer = new RaftMessageBuffer();

                        try {
                            while(true) {
                                int read = client.getInputStream().read(buffer);
                                messageBuffer.addToBuffer(buffer, read);

                                if (messageBuffer.hasNext()) {
                                    RaftMessage message = messageBuffer.next();

                                    switch (message.getType()) {
                                        case CHAT_MESSAGE: {
                                            if (state == State.LEADER) {
                                                log.add(new LogEntry(currentTerm, message.getChatMessage()));
                                                client.getOutputStream().write(RaftMessage.hostnameList(new ArrayList<>()).toByteArray());
                                            } else if (leaderId != null) {
                                                String[] leader = {leaderId.getHostName() + ":" + leaderId.getPort()};
                                                client.getOutputStream().write(RaftMessage.hostnameList(Arrays.asList(leader)).toByteArray());
                                            } else {
                                                client.getOutputStream().write(RaftMessage.hostnameList(new ArrayList<>()).toByteArray());
                                            }
                                        }
                                    }
                                }
                            }
                        } catch (IOException e) {
                            return;
                        }
                    }
                }).start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
