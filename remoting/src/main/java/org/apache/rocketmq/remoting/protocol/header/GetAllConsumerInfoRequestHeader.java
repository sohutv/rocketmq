package org.apache.rocketmq.remoting.protocol.header;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class GetAllConsumerInfoRequestHeader implements CommandCustomHeader {

    private boolean excludeSystemGroup;

    @Override
    public void checkFields() throws RemotingCommandException {
        // To change body of implemented methods use File | Settings | File
        // Templates.
    }

    public boolean isExcludeSystemGroup() {
        return excludeSystemGroup;
    }

    public void setExcludeSystemGroup(boolean excludeSystemGroup) {
        this.excludeSystemGroup = excludeSystemGroup;
    }
}
