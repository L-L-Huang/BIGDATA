package model;

public class DDBook {
    private String uuid = "";
    private String area = "";
    private String channel = "";
    private String standard = "";
    private long time;

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getStandard() {
        return standard;
    }

    public void setStandard(String standard) {
        this.standard = standard;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "DDBook{" +
                "uuid='" + uuid + '\'' +
                ", area='" + area + '\'' +
                ", channel='" + channel + '\'' +
                ", standard='" + standard + '\'' +
                ", time=" + time +
                '}';
    }
}
