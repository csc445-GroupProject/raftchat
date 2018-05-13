package edu.oswego.raftchat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;

public class RaftMessageBuffer {
    private ArrayDeque<RaftMessage> bufferedMessages;
    private int buffered;
    private ByteBuffer buffer;

    public RaftMessageBuffer() {
        this(32768);
    }

    public RaftMessageBuffer(int bufferSize) {
        bufferedMessages = new ArrayDeque<>();
        buffered = 0;
        buffer = ByteBuffer.allocate(bufferSize);
    }

    public void addToBuffer(byte[] bytes) { addToBuffer(bytes, bytes.length); }

    public void addToBuffer(byte[] bytes, int count) {
        buffer.put(bytes, 0, count);
        buffered += count;
        while(buffered > 4) {
            int size = buffer.getInt(0);
            if(buffered >= size) {
                buffer.position(0);
                byte[] messageBytes = new byte[size];
                buffer.get(messageBytes);
                buffer.compact();
                buffered -= size;

                try {
                    bufferedMessages.add(RaftMessage.fromByteArray(messageBytes));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                buffer.position(buffered);
                break;
            }
        }
    }

    public boolean hasNext() { return !bufferedMessages.isEmpty(); }

    public RaftMessage next() { return bufferedMessages.poll(); }
}