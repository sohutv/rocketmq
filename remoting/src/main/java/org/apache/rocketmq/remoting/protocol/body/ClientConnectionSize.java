package org.apache.rocketmq.remoting.protocol.body;

public class ClientConnectionSize {
    // 客户端大小
    private int clientSize;
    // 客户端链接大小
    private int clientConnectionSize;
    // 系统客户端大小
    private int systemClientSize;
    // 系统客户端链接大小
    private int systemClientConnectionSize;

    public int getClientSize() {
        return clientSize;
    }

    public void setClientSize(int clientSize) {
        this.clientSize = clientSize;
    }

    public void addClientSize(int clientSize) {
        this.clientSize += clientSize;
    }

    public int getClientConnectionSize() {
        return clientConnectionSize;
    }

    public void setClientConnectionSize(int clientConnectionSize) {
        this.clientConnectionSize = clientConnectionSize;
    }

    public void addClientConnectionSize(int clientConnectionSize) {
        this.clientConnectionSize += clientConnectionSize;
    }

    public int getSystemClientSize() {
        return systemClientSize;
    }

    public void setSystemClientSize(int systemClientSize) {
        this.systemClientSize = systemClientSize;
    }

    public void addSystemClientSize(int systemClientSize) {
        this.systemClientSize += systemClientSize;
    }

    public int getSystemClientConnectionSize() {
        return systemClientConnectionSize;
    }

    public void setSystemClientConnectionSize(int systemClientConnectionSize) {
        this.systemClientConnectionSize = systemClientConnectionSize;
    }

    public void addSystemClientConnectionSize(int systemClientConnectionSize) {
        this.systemClientConnectionSize += systemClientConnectionSize;
    }
}
