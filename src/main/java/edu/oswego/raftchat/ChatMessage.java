package edu.oswego.raftchat;

import java.io.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class ChatMessage {
    private Instant timestamp;
    private String username;
    private String text;

    public ChatMessage(Instant timestamp, String username, String text) {
        this.timestamp = timestamp;
        this.username = username;
        this.text = text;
    }

    public ChatMessage(String username, String text) {
        timestamp = Instant.EPOCH;
        this.username = username;
        this.text = text;
    }

    public static ChatMessage fromByteArray(byte[] bytes) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(bais);

        Instant timestamp = Instant.ofEpochMilli(in.readLong());
        String username = in.readUTF();
        String text = in.readUTF();

        return new ChatMessage(timestamp, username, text);
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public String getUsername() {
        return username;
    }

    public String getText() {
        return text;
    }

    public byte[] toByteArray() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        try {
            out.writeLong(timestamp.toEpochMilli());
            out.writeUTF(username);
            out.writeUTF(text);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    @Override
    public String toString() {
        ZonedDateTime localTime = timestamp.atZone(ZoneId.systemDefault());
        String timeString = localTime.format(DateTimeFormatter.ofPattern("H:m:s"));

        return String.format("(%s) %s: %s",timeString, username, text);
    }
}
