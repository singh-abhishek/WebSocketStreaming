package com.abhishek.streaming.client;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;
import javax.websocket.ClientEndpoint;
import javax.websocket.CloseReason;
import javax.websocket.DeploymentException;
import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import org.glassfish.tyrus.client.ClientManager;

@ClientEndpoint
public class StreamingClientEndpoint {
    private static CountDownLatch latch;
    private Logger logger = Logger.getLogger(this.getClass().getName());
    private long startTime, endTime;
    File file = new File("C:\\EclipseWorkspace\\Files\\output_data.psv");
    BufferedWriter br;

    @OnOpen
    public void onOpen(Session session) {
        startTime = System.currentTimeMillis();
        logger.info("Connected ... " + session.getId());
        try {
            session.getBasicRemote().sendText("start");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            br = new BufferedWriter(new FileWriter(file));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @OnMessage
    public String onMessage(String receivedMessage, Session session) throws IOException {
        br.write(receivedMessage);
        br.newLine();
        if (receivedMessage.equalsIgnoreCase(""))
        {
            return "stop";
        }
        return "";
    }

    @OnClose
    public void onClose(Session session, CloseReason closeReason) {
        logger.info(String.format("Session %s close because of %s", session.getId(), closeReason));
        latch.countDown();
        endTime = System.currentTimeMillis();
        System.out.println("total time taken by client = "+(endTime - startTime)+" msec");
        try {
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        latch = new CountDownLatch(1);
        ClientManager client = ClientManager.createClient();
        try {
            client.connectToServer(StreamingClientEndpoint.class, new URI("ws://localhost:8025/websockets/streaming"));
            latch.await();
        } catch (DeploymentException | URISyntaxException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
