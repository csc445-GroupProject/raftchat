package edu.oswego.raftchat;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import javafx.application.Application;
import javafx.stage.Stage;

import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class App extends Application {
    static InetSocketAddress initAddress;
    BlockingQueue<ChatMessage> chatQueue = new LinkedBlockingQueue<>();

    private static class Options {
        @Parameter(names = "-no-client", description = "Run raft peer without chat client")
        boolean noClient = false;

        @Parameter(names = "-print", description = "Print chat to stdout")
        boolean print = false;

        @Parameter(names = {"-p", "-port"}, description = "Run raft node on specified port")
        Integer port;

        @Parameter(names = {"-s", "-server"}, description = "Address of node to connect to")
        String server;
    }

    public static void main(String[] args) {
        Options options = new Options();
        JCommander.newBuilder()
                .addObject(options)
                .build()
                .parse(args);

        if (options.server != null) {
            Matcher addressMatcher = Pattern.compile("(.*?):(\\d+)").matcher(options.server);
            String host = addressMatcher.group(1);
            int port = Integer.parseInt(addressMatcher.group(2));

            initAddress = new InetSocketAddress(host, port);
        }

        if (options.noClient) {
            System.out.println("Running raft node without associated chat client.");
        } else {
            launch(args);
        }
    }

    @Override
    public void start(Stage stage) {

    }
}
