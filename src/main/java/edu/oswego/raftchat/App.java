package edu.oswego.raftchat;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import edu.oswego.raftchat.components.ConnectDialog;
import javafx.application.Application;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;

import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class App extends Application {
    private static Options options;
    private static InetSocketAddress initAddress;
    static BlockingQueue<ChatMessage> chatQueue = new LinkedBlockingQueue<>();

    private String username;
    private TextField chatField;
    private TextArea chatArea;

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
        options = new Options();
        JCommander.newBuilder()
                .addObject(options)
                .build()
                .parse(args);

        if (options.server != null) {
            Matcher addressMatcher = Pattern.compile("^(.*):(\\d+)$").matcher(options.server.trim());
            if (addressMatcher.matches()) {
                String host = addressMatcher.group(1);
                int port = Integer.parseInt(addressMatcher.group(2));

                initAddress = new InetSocketAddress(host, port);
            }
        }

        if (options.noClient) {
            System.out.println("Running raft node without associated chat client.");
        } else {
            launch(args);
        }
    }

    @Override
    public void start(Stage stage) {
        ConnectDialog connectDialog;

        if(initAddress != null) {
            connectDialog = new ConnectDialog(initAddress.getHostName(), initAddress.getPort());
        } else {
            connectDialog = new ConnectDialog();
        }

        new ConnectDialog().showAndWait().ifPresent(m -> {
            username = m.get("username");
            String addr = m.get("server");

            int port = Integer.parseInt(m.get("port"));

            chatField = new TextField();
            chatArea = new TextArea();
            VBox.setVgrow(chatArea, Priority.ALWAYS);
            chatArea.setEditable(false);
            HBox.setHgrow(chatField, Priority.ALWAYS);
            Button sendButton = new Button("Send");
            HBox inputBox = new HBox(10, chatField, sendButton);


            VBox root = new VBox(10, chatArea, inputBox);
            root.setPadding(new Insets(10, 10, 10, 10));
            stage.setScene(new Scene(root));
            chatField.requestFocus();
            stage.show();
        });
    }
}
