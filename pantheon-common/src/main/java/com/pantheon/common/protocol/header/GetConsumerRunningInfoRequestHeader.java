

package com.pantheon.common.protocol.header;


import com.pantheon.remoting.CommandCustomHeader;
import com.pantheon.remoting.annotation.CFNotNull;
import com.pantheon.remoting.exception.RemotingCommandException;

public class GetConsumerRunningInfoRequestHeader implements CommandCustomHeader {

    @CFNotNull
    private String clientId;


    @Override
    public void checkFields() throws RemotingCommandException {
    }


    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

}
