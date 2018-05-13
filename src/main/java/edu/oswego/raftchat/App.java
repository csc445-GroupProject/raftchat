package edu.oswego.raftchat;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import edu.oswego.raftchat.components.ConnectDialog;
import javafx.application.Application;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.input.KeyCode;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.time.Instant;
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
    private Socket leaderSocket;

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
            try {
                String[] address = options.server.trim().split(":");
                initAddress = new InetSocketAddress(address[0], Integer.parseInt(address[1]));
            } catch (ArrayIndexOutOfBoundsException|NumberFormatException e) {
                e.printStackTrace();
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

        connectDialog.showAndWait().ifPresent(m -> {
            username = m.get("username");

            if(m.containsKey("server")) {
                String host = m.get("server");
                int port = Integer.parseInt(m.get("port"));
                try {
                    leaderSocket = new Socket(host, port);
                } catch (IOException e) {
                    new Alert(Alert.AlertType.ERROR, String.format("Could not connect to node %s:%d", host, port)).showAndWait();
                }
            }

            chatField = new TextField();
            chatArea = new TextArea();
            VBox.setVgrow(chatArea, Priority.ALWAYS);
            chatArea.setEditable(false);
            HBox.setHgrow(chatField, Priority.ALWAYS);
            Button sendButton = new Button("Send");
            HBox inputBox = new HBox(10, chatField, sendButton);

            sendButton.setOnMouseClicked(mouseEvent -> {
                sendChatMessage();
            });

            chatField.setOnKeyPressed(keyEvent -> {
                if(keyEvent.getCode() == KeyCode.ENTER)
                    sendChatMessage();
            });

            VBox root = new VBox(10, chatArea, inputBox);
            root.setPadding(new Insets(10, 10, 10, 10));
            stage.setScene(new Scene(root, 640, 480));
            stage.setTitle("raftchat");
            chatField.requestFocus();
            stage.show();

            Thread chatAppendThread = new Thread(() -> {
                while(true) {
                    try {
                        chatArea.appendText(chatQueue.take().toString() + "\n");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }, "ChatAppendThread");

            chatAppendThread.setDaemon(true);
            chatAppendThread.start();
        });
    }

    // vv placeholder please change
    private void sendChatMessage() {
        String text = chatField.getText().trim();
        chatQueue.add(new ChatMessage(Instant.now(), username, text));
        chatField.clear();
    }
}
