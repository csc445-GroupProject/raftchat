package edu.oswego.raftchat;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class RaftNode implements Runnable {
    private String hostName;
    private int initialPort;
    private int currentTerm = 0;
    private InetSocketAddress me;
    private InetSocketAddress votedFor;
    private List<LogEntry> log = Collections.synchronizedList(new ArrayList<>());
    private AtomicReference<State> state = new AtomicReference<>();
    private int commitIndex = -1;
    private int lastApplied = -1;
    private ServerSocket serverSocket;
    private Set<Socket> clients = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private Map<InetSocketAddress, Socket> peers = new ConcurrentHashMap<>();
    private InetSocketAddress leaderId;
    private int votesForCurrentTerm = 0;
    private int numberOfVotes = 0;
    private int majority = 0;

    // leader state
    private Map<InetSocketAddress, Integer> nextIndex = new ConcurrentHashMap<>();
    private Map<InetSocketAddress, Integer> matchIndex = new ConcurrentHashMap<>();

    // queue to send to client
    private BlockingQueue<ChatMessage> clientQueue;

    // incoming message queue
    private class MessageEntry {
        InetSocketAddress source;
        RaftMessage value;

        MessageEntry(InetSocketAddress source, RaftMessage value) {
            this.source = source;
            this.value = value;
        }
    }
    private BlockingQueue<MessageEntry> messageQueue = new LinkedBlockingQueue<>();

    enum State {
        LEADER, CANDIDATE, FOLLOWER
    }

    /* CONSTRUCTORS
    --------------------------------------------------------------------------------------------------------------------
     */
    public RaftNode(String hostName, int initialPort, BlockingQueue<ChatMessage> clientQueue) throws IOException {
        this(hostName, initialPort, clientQueue, 0);
    }

    public RaftNode(String hostName, int initialPort, BlockingQueue<ChatMessage> clientQueue, int serverSocketPort) throws IOException {

        // get external ip
        URL whatismyip = new URL("http://checkip.amazonaws.com");
        BufferedReader in = new BufferedReader(new InputStreamReader(
                whatismyip.openStream()));

        String ip = in.readLine();

        this.state.set(State.FOLLOWER);
        this.clientQueue = clientQueue;
        serverSocket = new ServerSocket(serverSocketPort);
        this.hostName = hostName;
        this.initialPort = initialPort;
        me = new InetSocketAddress(ip, serverSocketPort);

    }

    /*
    --------------------------------------------------------------------------------------------------------------------
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
     */


    /*

     */
    @Override
    public void run() {
        long electionDelay = ThreadLocalRandom.current().nextLong(150, 300);
        Map<InetSocketAddress, Socket> newPeers = new HashMap<>();
        try {
            newPeers.put(new InetSocketAddress(hostName, initialPort), new Socket(hostName, initialPort));
            updatePeers(newPeers);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Could not connect to initial peer");
            return;
        }

        new Thread(this::handleNewCommits).start();

        while (true) {
            switch (state.get()) {
                case FOLLOWER: {
                    ScheduledExecutorService electionTimeout = Executors.newSingleThreadScheduledExecutor();
                    ScheduledFuture timeoutFuture = electionTimeout.schedule(() -> {
                        state.compareAndSet(State.FOLLOWER, State.CANDIDATE);
                    }, electionDelay, TimeUnit.MILLISECONDS);

                    while(state.get() == State.FOLLOWER) {
                        try {
                            RaftMessage newMessage = messageQueue.poll(100, TimeUnit.MILLISECONDS).value;
                            if(newMessage != null) {
                                currentTerm = newMessage.getTerm() > currentTerm? newMessage.getTerm() : currentTerm;
                                switch(newMessage.getType()) {
                                    case APPEND_REQUEST: {
                                        timeoutFuture.cancel(false);
                                        leaderId = new InetSocketAddress(newMessage.getLeaderHostname(), newMessage.getLeaderPort());
                                        timeoutFuture = electionTimeout.schedule(() -> {
                                            state.compareAndSet(State.FOLLOWER, State.CANDIDATE);
                                        }, electionDelay, TimeUnit.MILLISECONDS);

                                        new Thread(() -> {
                                            int prevLogIndex = newMessage.getPrevLogIndex();
                                            int prevLogTerm = newMessage.getPrevLogTerm();
                                            LogEntry prevEntry = log.get(prevLogIndex);

                                            boolean success = true;
                                            if(newMessage.getTerm() < currentTerm || prevEntry == null || prevEntry.getTerm() != prevLogTerm)
                                                success = false;
                                            else if(!newMessage.getEntries().isEmpty()) {
                                                log.subList(prevLogIndex + 1, log.size()).clear();
                                                log.addAll(newMessage.getEntries());
                                            }

                                            if(newMessage.getLeaderCommit() > commitIndex)
                                                commitIndex = Math.min(newMessage.getLeaderCommit(), log.size() - 1);


                                            Socket leader = peers.get(leaderId);
                                            try {
                                                leader.getOutputStream().write(RaftMessage.appendResponse(currentTerm, success).toByteArray());
                                                leader.getOutputStream().flush();
                                            } catch (IOException e) {
                                                e.printStackTrace();
                                            }
                                        }).start();
                                        break;
                                    }
                                    case VOTE_REQUEST: {
                                        new Thread(() -> {
                                            InetSocketAddress candidateId = new InetSocketAddress(newMessage.getCandidateHostname(), newMessage.getCandidatePort());
                                            int lastLogIndex = newMessage.getLastLogIndex();
                                            int lastLogTerm = newMessage.getLastLogTerm();

                                            boolean voteGranted = newMessage.getTerm() >= currentTerm
                                                    && (votedFor == null || votedFor.equals(candidateId))
                                                    && ((lastLogIndex == log.size() - 1 && log.get(lastLogIndex).getTerm() <= lastLogTerm)
                                                        || lastLogIndex >= log.size());

                                            Socket candidate = peers.get(candidateId);
                                            try {
                                                candidate.getOutputStream().write(RaftMessage.voteResponse(currentTerm, voteGranted).toByteArray());
                                                candidate.getOutputStream().flush();
                                            } catch (IOException e) {
                                                e.printStackTrace();
                                            }
                                        }).start();
                                        break;
                                    }
                                }
                            }
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    break;
                }
                case CANDIDATE: {
                    AtomicBoolean timedOut = new AtomicBoolean(false);

                    int votes = 1;
                    votedFor = me;

                    ScheduledExecutorService electionTimeout = Executors.newSingleThreadScheduledExecutor();
                    ScheduledFuture timeoutFuture = electionTimeout.schedule(() -> {
                        timedOut.compareAndSet(false, true);
                    }, electionDelay, TimeUnit.MILLISECONDS);
                    currentTerm++;

                    for(final Socket p : peers.values())
                        new Thread(() -> requestVote(p)).start();
                    while (state.get() == State.CANDIDATE && !timedOut.get()) {
                        try {
                            RaftMessage newMessage = messageQueue.peek().value;
                            if (newMessage != null) {
                                if(newMessage.getType() == MessageType.APPEND_REQUEST || newMessage.getTerm() > currentTerm) {
                                    currentTerm = newMessage.getTerm();
                                    state.compareAndSet(State.CANDIDATE, State.FOLLOWER);
                                    break;
                                }
                                switch (newMessage.getType()) {
                                    case VOTE_RESPONSE: {
                                        messageQueue.take();
                                        if (newMessage.getVoteGranted()) {
                                            votes++;
                                            if(votes >= majority()) {
                                                // convert to leader
                                                state.compareAndSet(State.CANDIDATE, State.LEADER);
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
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

                    while (state.get() == State.LEADER) {
                        for (final Socket p : peers.values()) {
                            new Thread(() -> {
                                try {
                                    p.getOutputStream().write(heartbeat().toByteArray());
                                    p.getOutputStream().flush();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }).start();
                        }

                        for (InetSocketAddress a : peers.keySet()) {
                            new Thread(() -> {
                                if (log.size() - 1 >= nextIndex.get(a)) {
                                    int prevIndex = nextIndex.get(a) - 1;
                                    RaftMessage appendMessage = RaftMessage.appendRequest(currentTerm, me.getHostName(),
                                            me.getPort(), prevIndex, log.get(prevIndex).getTerm(), log.subList(prevIndex + 1, log.size()), commitIndex);
                                    try {
                                        OutputStream out = peers.get(a).getOutputStream();
                                        out.write(appendMessage.toByteArray());
                                        out.flush();
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                }

                                try {
                                    MessageEntry response = messageQueue.poll(300, TimeUnit.MILLISECONDS);
                                    if (response.value.getSuccess()) {
                                        matchIndex.put(response.source, log.size() - 1);
                                        nextIndex.put(response.source, log.size());
                                    } else {
                                        nextIndex.put(response.source, nextIndex.get(response.source) - 1);
                                    }
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }).start();
                        }
                    }

                    int n = log.size() - 1;
                    for(; n >= commitIndex; n--) {
                        int count = 0;
                        if(log.get(n).getTerm() == currentTerm) {
                            for(Integer i : matchIndex.values()) {
                                if(i >= n)
                                    count++;
                            }
                            if(count >= majority())
                                break;
                        }
                    }

                    commitIndex = n;
                    break;
                }
            }
        }
    }

    private void handleNewCommits() {
        while(true) {
            while (commitIndex > lastApplied) {
                lastApplied++;

                LogEntry entry = log.get(lastApplied);
                switch (entry.getType()) {
                    case CHAT: {
                        clientQueue.offer(entry.getMessage());
                    }
                    case CONFIG: {
                        Set<InetSocketAddress> config = entry.getConfig();
                        Map<InetSocketAddress, Socket> newPeers = new HashMap<>();

                        for(InetSocketAddress a : config) {
                            try {
                                newPeers.put(a, new Socket(a.getAddress(), a.getPort()));
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }

                        updatePeers(newPeers);
                    }
                }
            }
        }
    }

    private void updatePeers(Map<InetSocketAddress, Socket> newPeers) {
        for(Socket peer : peers.values()) {
            try {
                peer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        peers.clear();
        peers.entrySet().addAll(newPeers.entrySet());

        for(final Map.Entry<InetSocketAddress, Socket> peer : peers.entrySet()) {
            new Thread(() -> {
                Socket p = peer.getValue();
                RaftMessageBuffer messageBuffer = new RaftMessageBuffer();
                byte[] buff = new byte[8192];
                int read;

                while (true) {
                    try {
                        while(!messageBuffer.hasNext()) {
                            read = p.getInputStream().read(buff);
                            messageBuffer.addToBuffer(buff, read);
                        }
                    } catch (IOException e) {
                        if(state.get() == State.LEADER) {
                            Set<InetSocketAddress> config = new HashSet<>(peers.keySet());
                            config.removeIf(a -> peers.get(a).equals(p));
                            log.add(new LogEntry(LogEntry.Type.CONFIG, currentTerm, null, config));
                        }
                        return;
                    }

                    messageQueue.add(new MessageEntry(peer.getKey(), messageBuffer.next()));
                }
            }).start();
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
                                            if (state.get() == State.LEADER) {
                                                log.add(new LogEntry(LogEntry.Type.CHAT, currentTerm, message.getChatMessage(), null));
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
        InetSocketAddress peerAddress = (InetSocketAddress) socket.getRemoteSocketAddress();
        int index = nextIndex.get(peerAddress);

        //NULL MUST BE REPLACED WITH LIST OF ENTRIES ON LINE 168, WAS ONLY ADDED FOR COMPILABILITY PURPOSES.
        return RaftMessage.appendRequest(currentTerm, me.getHostName(), me.getPort(), lastApplied, log.get(lastApplied).getTerm(), log.subList(index, log.size()), commitIndex);

    }

    public RaftMessage heartbeat() {
        return RaftMessage.appendRequest(currentTerm, me.getHostName(), me.getPort(), lastApplied, log.get(lastApplied).getTerm(), new ArrayList<>(), commitIndex);
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
        RaftMessage message = RaftMessage.voteRequest(currentTerm, me.getHostName(), me.getPort(), log.size() - 1, log.get(log.size() - 1).getTerm());
        try {
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            out.write(message.toByteArray());
            out.flush();
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
        state.compareAndSet(State.FOLLOWER, State.CANDIDATE);
        currentTerm++;
        votedFor = me;
        int size = peers.size();
        Random r = new Random();
        int electionTimeout = r.nextInt(150) + 150;
        Socket[] peerSockets = new Socket[size];
        peers.values().toArray(peerSockets);

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

        if(numberOfVotes > majority()) {
            state.compareAndSet(State.CANDIDATE, State.LEADER);
            numberOfVotes = 0;
        }
    }

    private int majority() {
        return (peers.size() + 1) / 2 + 1;
    }

    /*
    --------------------------------------------------------------------------------------------------------------------
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
     */
}
