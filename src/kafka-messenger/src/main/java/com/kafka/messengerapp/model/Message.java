package com.kafka.messengerapp.model;

public class Message {
    private String fromUserId;

    private String toUserId;

    private String messageTxt;

    public Message(String fromUserId, String toUserId, String messageTxt) {
        this.fromUserId = fromUserId;
        this.toUserId = toUserId;
        this.messageTxt = messageTxt;
    }

    public String getMessageTxt() {
        return messageTxt;
    }

    public void setMessageTxt(String messageTxt) {
        this.messageTxt = messageTxt;
    }

    public String getFromUserId() {
        return fromUserId;
    }

    public void setFromUserId(String fromUserId) {
        this.fromUserId = fromUserId;
    }

    public String getToUserId() {
        return toUserId;
    }

    public void setToUserId(String toUserId) {
        this.toUserId = toUserId;
    }
}
