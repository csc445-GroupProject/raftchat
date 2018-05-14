package edu.oswego.raftchat;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.Set;

public class LogEntry {
    public enum Type {
        CHAT, CONFIG
    }

    private Type type;

    private int term;
    private ChatMessage message;

    private Set<InetSocketAddress> config;

    public LogEntry(Type type, int term, ChatMessage message, Set<InetSocketAddress> config) {
        this.type = type;
        this.term = term;
        this.message = message;
        this.config = config;
    }

    public Type getType() {
        return type;
    }

    public Set<InetSocketAddress> getConfig() {
        return config;
    }

    public int getTerm() {
        return term;
    }

    public ChatMessage getMessage() { return message; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogEntry logEntry = (LogEntry) o;
        return term == logEntry.term &&
                Objects.equals(message, logEntry.message);
    }

    @Override
    public int hashCode() {

        return Objects.hash(term, message);
    }
}
