package edu.oswego.raftchat;

import java.util.Objects;

public class LogEntry {
    private int term;
    private ChatMessage message;

    public LogEntry(int term, ChatMessage message) {
        this.term = term;
        this.message = message;
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
