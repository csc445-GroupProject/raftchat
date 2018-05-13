package edu.oswego.raftchat;

import java.io.*;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class RaftMessage {
    private MessageType type;
    private Integer term;
    private Integer leaderId;
    private Integer prevLogIndex;
    private Integer prevLogTerm;
    private List<LogEntry> entries;
    private ChatMessage chatMessage;
    private Integer leaderCommit;
    private Integer candidateId;
    private Integer lastLogIndex;
    private Integer lastLogTerm;
    private Boolean success;
    private Boolean voteGranted;
    private List<String> hostnames;

    public MessageType getType() {
        return type;
    }

    public Integer getTerm() {
        return term;
    }

    public Integer getLeaderId() {
        return leaderId;
    }

    public Integer getPrevLogIndex() {
        return prevLogIndex;
    }

    public Integer getPrevLogTerm() {
        return prevLogTerm;
    }

    public List<LogEntry> getEntries() {
        return entries;
    }

    public Integer getLeaderCommit() {
        return leaderCommit;
    }

    public Integer getCandidateId() {
        return candidateId;
    }

    public Integer getLastLogIndex() {
        return lastLogIndex;
    }

    public Integer getLastLogTerm() {
        return lastLogTerm;
    }

    public Boolean getSuccess() {
        return success;
    }

    public Boolean getVoteGranted() {
        return voteGranted;
    }

    public List<String> getHostnames() {
        return hostnames;
    }

    private RaftMessage(MessageType type, Integer term, Integer leaderId, Integer prevLogIndex, Integer prevLogTerm,
                        List<LogEntry> entries, ChatMessage chatMessage, Integer leaderCommit, Integer candidateId,
                        Integer lastLogIndex, Integer lastLogTerm, Boolean success, Boolean voteGranted, List<String> hostnames) {
        this.type = type;
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries;
        this.chatMessage = chatMessage;
        this.leaderCommit = leaderCommit;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
        this.success = success;
        this.voteGranted = voteGranted;
        this.hostnames = hostnames;
    }

    /**
     * Creates a RaftMessage to request votes from peer nodes during leader elections
     * @param term the sending node's current election term
     * @param candidateId the node's location in the peer nodes arrayList of sockets
     * @param lastLogIndex the requesting node's last committed log entry
     * @param lastLogTerm the election term of the logs last entry
     * @return a RaftMessage that can be used for requesting votes from peer nodes
     */
    public static RaftMessage voteRequest(int term, int candidateId, int lastLogIndex, int lastLogTerm) {
        return new RaftMessage(MessageType.VOTE_REQUEST, term, null, null, null, null, null, null, candidateId, lastLogIndex,
                lastLogTerm, null, null, null);
    }

    /**
     * Creates a RaftMessage to respond to a voteRequest
     * @param term the node's current election term
     * @param voteGranted whether or not the vote was granted
     * @return A RaftMessage that will be sent to the requesting node
     */
    public static RaftMessage voteResponse(int term, boolean voteGranted) {
        return new RaftMessage(MessageType.VOTE_RESPONSE, term, null, null, null, null, null, null, null, null, null, null, voteGranted, null);
    }

    /**
     * Creates a RaftMessage requesting that peer nodes append the latest message to their logs
     * @param term the node's current election term
     * @param leaderId the leader's index in the peer node's List of sockets
     * @param prevLogIndex the last entry of the log that was added before the list of entries the leader node is
     *                     requesting be appended
     * @param prevLogTerm the term of the prevLogIndex entry
     * @param entries a List of LogEntries that are being requested to add to the log
     * @param leaderCommit The last entry in the log that is committed by the leader
     * @return A RaftMessage that can will be sent to all peer nodes requesting that they add thee sent entries to their
     * log
     */
    public static RaftMessage appendRequest(int term, int leaderId, int prevLogIndex, int prevLogTerm, List<LogEntry> entries, int leaderCommit) {
        return new RaftMessage(MessageType.APPEND_REQUEST, term, leaderId, prevLogIndex, prevLogTerm, entries, null, leaderCommit, null, null, null, null, null, null);
    }

    /**
     * Creates a RaftMessage that will be sent after a appendRequest is received, acknowledging whether or not the
     * entries were added to the log.
     * @param term The node's current election term
     * @param success whether or not the addition was successful
     * @return A RaftMessage to respond to an appendRequest message
     */
    public static RaftMessage appendResponse(int term, boolean success) {
        return new RaftMessage(MessageType.APPEND_RESPONSE, term, null, null, null, null, null, null, null, null, null, success, null, null);
    }

    /**
     * Creates a RaftMessage to send to the leader node to update the log
     * @param message the chat message to be added to the log
     * @return A RaftMessage to send to the leader node to append to the log
     */
    public static RaftMessage chatMessage(ChatMessage message) {
        return new RaftMessage(MessageType.CHAT_MESSAGE, null, null, null, null, null, message, null, null, null, null, null, null, null);
    }

    public static RaftMessage hostnameList(List<String> hostnames) {
        return new RaftMessage(MessageType.HOST_LIST, null, null, null, null, null, null, null, null, null, null, null, null, hostnames);
    }

    public static RaftMessage fromByteArray(byte[] bytes) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(bais);

        in.readInt(); // Ignore the message size field

        MessageType type;
        try {
            type = MessageType.values()[in.readInt()];
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new IOException(e);
        }

        switch (type) {
            case VOTE_REQUEST: {
                int term = in.readInt();
                int candidateId = in.readInt();
                int lastLogIndex = in.readInt();
                int lastLogTerm = in.readInt();
                return RaftMessage.voteRequest(term, candidateId, lastLogIndex, lastLogTerm);
            }
            case VOTE_RESPONSE: {
                int term = in.readInt();
                boolean voteGranted = in.readBoolean();
                return RaftMessage.voteResponse(term, voteGranted);
            }
            case APPEND_REQUEST: {
                int term = in.readInt();
                int leaderId = in.readInt();
                int prevLogIndex = in.readInt();
                int prevLogTerm = in.readInt();
                int size = in.readInt();
                List<LogEntry> entries = new ArrayList<>();

                for (int i = 0; i < size; i++) {
                    int entryTerm = in.readInt();
                    Instant timestamp = Instant.ofEpochMilli(in.readLong());
                    String username = in.readUTF();
                    String text = in.readUTF();
                    entries.add(new LogEntry(entryTerm, new ChatMessage(timestamp, username, text)));
                }

                int leaderCommit = in.readInt();
                return RaftMessage.appendRequest(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
            }
            case APPEND_RESPONSE: {
                int term = in.readInt();
                boolean success = in.readBoolean();
                return RaftMessage.appendResponse(term, success);
            }
            case CHAT_MESSAGE: {
                // skip message size and type
                return RaftMessage.chatMessage(ChatMessage.fromByteArray(Arrays.copyOfRange(bytes, 8, bytes.length)));
            }
            case HOST_LIST: {
                int size = in.readInt();
                List<String> hostnames = new ArrayList<>();

                for (int i = 0; i < size; i++) {
                    hostnames.add(in.readUTF());
                }
                return RaftMessage.hostnameList(hostnames);
            }
        }

        return null;
    }

    public byte[] toByteArray() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        try {
            switch (type) {
                case VOTE_REQUEST: {
                    out.writeInt(type.ordinal());
                    out.writeInt(term);
                    out.writeInt(candidateId);
                    out.writeInt(lastLogIndex);
                    out.writeInt(lastLogTerm);
                    break;
                }
                case VOTE_RESPONSE: {
                    out.writeInt(type.ordinal());
                    out.writeInt(term);
                    out.writeBoolean(voteGranted);
                    break;
                }
                case APPEND_REQUEST: {
                    out.writeInt(type.ordinal());
                    out.writeInt(term);
                    out.writeInt(leaderId);
                    out.writeInt(prevLogIndex);
                    out.writeInt(prevLogTerm);
                    out.writeInt(entries.size());

                    for (LogEntry e : entries) {
                        out.writeInt(e.getTerm());
                        out.write(e.getMessage().toByteArray());
                    }

                    out.writeInt(leaderCommit);
                    break;
                }
                case APPEND_RESPONSE: {
                    out.writeInt(type.ordinal());
                    out.writeInt(term);
                    out.writeBoolean(success);
                    break;
                }
                case CHAT_MESSAGE: {
                    out.writeInt(type.ordinal());
                    out.write(chatMessage.toByteArray());
                    break;
                }
                case HOST_LIST: {
                    out.writeInt(type.ordinal());
                    out.writeInt(hostnames.size());

                    for (String h : hostnames) {
                        out.writeUTF(h);
                    }
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        int size = baos.size();
        byte[] bytes = new byte[size + 4];
        ByteBuffer.wrap(bytes).putInt(size + 4).put(baos.toByteArray());
        return bytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RaftMessage that = (RaftMessage) o;
        return type == that.type &&
                Objects.equals(term, that.term) &&
                Objects.equals(leaderId, that.leaderId) &&
                Objects.equals(prevLogIndex, that.prevLogIndex) &&
                Objects.equals(prevLogTerm, that.prevLogTerm) &&
                Objects.equals(entries, that.entries) &&
                Objects.equals(chatMessage, that.chatMessage) &&
                Objects.equals(leaderCommit, that.leaderCommit) &&
                Objects.equals(candidateId, that.candidateId) &&
                Objects.equals(lastLogIndex, that.lastLogIndex) &&
                Objects.equals(lastLogTerm, that.lastLogTerm) &&
                Objects.equals(success, that.success) &&
                Objects.equals(voteGranted, that.voteGranted) &&
                Objects.equals(hostnames, that.hostnames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, term, leaderId, prevLogIndex, prevLogTerm, entries, chatMessage, leaderCommit, candidateId, lastLogIndex, lastLogTerm, success, voteGranted, hostnames);
    }
}
