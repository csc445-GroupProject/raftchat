package edu.oswego.raftchat;

import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Parameter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

public class RaftNode implements Runnable {
    private String hostName;
    private int initialPort;
    private int currentTerm = 0;
    private InetSocketAddress me;
    private InetSocketAddress votedFor;
    private List<LogEntry> log = Collections.synchronizedList(new ArrayList<>());
    private State state = State.FOLLOWER;
    private int commitIndex = -1;
    private int lastApplied = -1;
    private ServerSocket serverSocket;
    private Set<Socket> clients = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private Set<Socket> peers = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private InetSocketAddress leaderId;
    private int votesForCurrentTerm = 0;
    private int numberOfVotes = 0;
    private int majority = 0;

    // leader state
    Map<InetSocketAddress, Integer> nextIndex = new ConcurrentHashMap<>();
    Map<InetSocketAddress, Integer> matchIndex = new ConcurrentHashMap<>();

    // queue to send to client
    BlockingQueue<ChatMessage> clientQueue;

    enum State {
        LEADER, CANDIDATE, FOLLOWER
    }

    /* CONSTRUCTORS
    --------------------------------------------------------------------------------------------------------------------
     */
    public RaftNode(String hostName, int initialPort, BlockingQueue clientQueue) throws IOException {
        this(hostName, initialPort, clientQueue, 0);
    }

    public RaftNode(String hostName, int initialPort, BlockingQueue clientQueue, int serverSocketPort) throws IOException {
        this.clientQueue = clientQueue;
        serverSocket = new ServerSocket(serverSocketPort);
        this.hostName = hostName;
        this.initialPort = initialPort;
        me = new InetSocketAddress(this.hostName, this.initialPort);

    }

    /*
    --------------------------------------------------------------------------------------------------------------------
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
     */


    /*

     */
    @Override
    public void run() {
        while (true) {
            switch (state) {
                case FOLLOWER: {
                    //TODO respond to all incoming messages appropriately, forward messages to the leader that are sent
                    //TODO             by unknowing clients.


                    //TODO if timeout occurs start leader elections
                    break;
                }
                case CANDIDATE: {
                    //TODO listen for responses to leader election from other nodes
                    //TODO update the votes received if the peer actually voted for the node

                    //if appendEntries received will term >= currentTerm, become a follower

                    //TODO if timeout occurs reinitialize the leader elections
                    break;
                }
                case LEADER: {
                    //TODO Send initial heartbeat before any other correspondence, as soon as the node is chosen as leader

                    //TODO while (Still leader)
                    //TODO listen for commands from clients(NOT OTHER NODES)
                    //TODO append those changes to the local log
                    //TODO tell peer nodes to append the changes to their logs as well

                    //TODO if lastApplied>= next index, send Append Entries starting at nextIndex, if successful update
                    //TODO               nextindex and matchIndex for the follower

                    //TODO if append entries fails due to log inconsistency nextIndex-- and retry
                    break;
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

    /*
    --------------------------------------------------------------------------------------------------------------------
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
     */

    /* APPEND ENTRIES
    --------------------------------------------------------------------------------------------------------------------
     */


    public RaftMessage appendEntries(Socket socket) {
        InetAddress peerAddress = socket.getInetAddress();
        int index = nextIndex.get(peerAddress);


        //NULL MUST BE REPLACED WITH LIST OF ENTRIES ON LINE 168, WAS ONLY ADDED FOR COMPILABILITY PURPOSES.
        return RaftMessage.appendRequest(currentTerm,me.getHostName(), me.getPort(), lastApplied, log.get(lastApplied).getTerm(), null, commitIndex);

    }

    public RaftMessage sendHeartbeat() {
        return RaftMessage.appendRequest(currentTerm, me.getHostName(), me.getPort(), lastApplied, log.get(lastApplied).getTerm(), null, commitIndex);
    }

    public void sendAppendEntries(RaftMessage message, Socket socket) {

    }

    /*
    --------------------------------------------------------------------------------------------------------------------
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
     */


    /* LEADER ELECTIONS
    --------------------------------------------------------------------------------------------------------------------
    The following methods pertain to leader elections
    All methods below can be combined to elect a new leader node.
     */

    /**
     * A method to request a vote from other peer node Servers
     * This method should only be run when the node is in the candidate state.
     * @param socket The socket to send the request through
     */
    public void requestVote(Socket socket) {
        RaftMessage message = RaftMessage.voteRequest(currentTerm, me.getHostName(), me.getPort(), lastApplied, log.get(lastApplied).getTerm());
        try {
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            out.write(message.toByteArray());
        } catch(IOException e) {
            System.out.println("IOException has occurred while requesting vote");
        }
    }

    /**
     * A method to respond to a vote request message. This method alerts the node that another node has timed out and is
     * now lobbying to become the new leader. The node will vote for the requesting node as long as the requesting node
     * is at least on the same election term as the current node. If votedFor is either null or the requesting candidate
     * and the candidate's log is at least as up to date as the receiver;s log, the vote will be granted. Only one node
     * will be voted for.
     * @param message The RaftMessage the node is replying to.
     */
    public void respondToRequestVote(RaftMessage message) {
        boolean voteGranted = true;
        RaftMessage response;
        if(message.getType() == MessageType.VOTE_REQUEST) {
            if(message.getTerm() < currentTerm) {
                voteGranted = false;
            } else if(message.getTerm() == currentTerm){
                if(votedFor != null && votedFor != message.getCandidateId()) {
                    voteGranted = false;
                }
            } else {
                if(message.getLastLogIndex() < lastApplied) {
                    voteGranted = false;
                }
            }
            response = RaftMessage.voteResponse(currentTerm, voteGranted);
            try {
                Socket replySocket = new Socket(message.getCandidateHostname(), message.getCandidatePort());
                DataOutputStream out = new DataOutputStream(replySocket.getOutputStream());
                out.write(response.toByteArray());
            } catch(IOException e) {
                System.out.println("IOException has occurred while sending vote response.");
            }
        }
    }

    /**
     * Followers will perform this method if they receive a timeout error from their leader's socket, and no other
     * voteRequest messages. Nodes who perform this method will automatically be placed in the running for next election
     * term's running for leader. Nodes who perform this method will be allowed to try again granted they do not receive
     * at least the majority of votes and they are the first node to time out after the initial run.
     */
    public void startElection() {
        state = State.CANDIDATE;
        currentTerm++;
        votedFor = me;
        int size = peers.size();
        Random r = new Random();
        int electionTimeout = r.nextInt(150) + 150;
        Socket[] peerSockets = new Socket[size];
        peers.toArray(peerSockets);

        for (int i=0; i<size; i++) {
            final int iFinal = i;
            new Thread(()->{
                try {
                    peerSockets[iFinal].setSoTimeout(electionTimeout);
                    requestVote(peerSockets[iFinal]);
                } catch(IOException e) {
                    System.out.println("Socket has Timed out:");
                }
            }, "Socket " + iFinal + " Thread").start();
        }
    }

    /**
     * updates the number of votes for leader elections. This will be reset for every new election term, as well as if
     * another node becomes elected for the same election term.
     */
    private void updateVoteCount() {
        numberOfVotes++;

        if(numberOfVotes > majority) { // doesn't work right now because majority isn't related to peers yet.
            state = State.LEADER;
            numberOfVotes = 0;
        }
    }

    /*
    --------------------------------------------------------------------------------------------------------------------
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
     */
}
