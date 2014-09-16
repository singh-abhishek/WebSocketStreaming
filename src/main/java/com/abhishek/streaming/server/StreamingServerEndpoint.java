package com.abhishek.streaming.server;

import java.io.*;
import java.util.logging.Logger;
import javax.websocket.CloseReason;
import javax.websocket.CloseReason.CloseCodes;
import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;

@ServerEndpoint(value = "/streaming")
public class StreamingServerEndpoint {
    private Logger logger = Logger.getLogger(this.getClass().getName());
    BufferedReader br;
    long lno=0;

    @OnOpen
    public void onOpen(Session session) {
        logger.info("Connected ... " + session.getId());
    }

    @OnMessage
    public String onMessage(String receivedMessage, Session session) throws IOException {
        String line,totalLine="";
        switch (receivedMessage) {
        case "start":
            logger.info("Streaming started...");
            File file = new File("C:\\EclipseWorkspace\\Files\\input_data1.psv");
            try {
                br = new BufferedReader(new FileReader(file));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        break;
        case "stop":
            logger.info("Streaming stopped.");
            try {
                session.close(new CloseReason(CloseCodes.NORMAL_CLOSURE, "Streaming finished"));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        totalLine="";
        for(int i=0;(i<9) && ((line = br.readLine()) != null);i++) {
                totalLine += line;
                lno++;
        }
        return totalLine;
    }

    @OnClose
    public void onClose(Session session, CloseReason closeReason) {
        logger.info(String.format("Session %s closed because of %s", session.getId(), closeReason));
        try {
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
