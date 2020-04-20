package frauddetection.entity;

import java.io.Serializable;
import java.util.UUID;

public final class Transaction implements Serializable {

    private String eventType;

    private UUID uid;

    private long timestamp;

    private String ip;

    private UUID impressionId;

    public Transaction() { }

    public Transaction(String eventType, UUID uid, long timestamp, String ip, UUID impressionId) {
        this.eventType = eventType;
        this.uid = uid;
        this.timestamp = timestamp;
        this.ip = ip;
        this.impressionId = impressionId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public UUID getUid() {
        return uid;
    }

    public void setUid(UUID uid) {
        this.uid = uid;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public UUID getImpressionId() {
        return impressionId;
    }

    public void setImpressionId(UUID impressionId) {
        this.impressionId = impressionId;
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "eventType='" + eventType + '\'' +
                ", uid=" + uid +
                ", timestamp=" + timestamp +
                ", ip='" + ip + '\'' +
                ", impressionId=" + impressionId +
                '}';
    }
}
