package edu.oswego.raftchat.components;

import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.scene.Node;
import javafx.scene.control.*;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;

import java.util.HashMap;
import java.util.Map;

public class ConnectDialog extends Dialog<Map<String, String>> {
    private static final ButtonType connectButtonType = new ButtonType("Connect", ButtonBar.ButtonData.OK_DONE);

    public ConnectDialog() {
        this("pi.cs.oswego.edu", 2706);
    }

    public ConnectDialog(String serverName, int portNum) {
        super();

        getDialogPane().getButtonTypes().addAll(connectButtonType, ButtonType.CANCEL);
        setTitle("Connect to Server");

        VBox mainBox = new VBox();
        mainBox.setPadding(new Insets(10, 75, 10, 10));
        mainBox.setSpacing(10);

        HBox serverBox = new HBox();
        serverBox.setSpacing(10);

        TextField username = new TextField();
        username.setPromptText("Username");
        TextField server = new TextField();
        server.setText(serverName);
        server.setPromptText("Server");
        TextField port = new TextField();
        port.setMinWidth(40);
        port.setText(Integer.toString(portNum));
        port.setPromptText("Port");
        port.prefWidthProperty().bind(serverBox.widthProperty().divide(3));

        serverBox.getChildren().addAll(server, port);

        CheckBox connectToNodeCheck = new CheckBox("Connect to existing node");
        server.disableProperty().bind(connectToNodeCheck.selectedProperty().not());
        port.disableProperty().bind(connectToNodeCheck.selectedProperty().not());

        mainBox.getChildren().addAll(username, connectToNodeCheck, serverBox);

        getDialogPane().setContent(mainBox);

        Node connectButton = getDialogPane().lookupButton(connectButtonType);
        connectButton.disableProperty().bind(username.textProperty().isEmpty()
                .and(connectToNodeCheck.selectedProperty().not())
                .or(server.textProperty().isEmpty()
                .or(port.textProperty().isEmpty())));

        Platform.runLater(username::requestFocus);

        setResultConverter(type -> {
            if(type == connectButtonType) {
                Map<String, String> result = new HashMap<>();
                result.put("username", username.getText().trim());
                result.put("server", server.getText().trim());
                result.put("port", port.getText().trim());
                return result;
            }
            return null;
        });
    }
}
