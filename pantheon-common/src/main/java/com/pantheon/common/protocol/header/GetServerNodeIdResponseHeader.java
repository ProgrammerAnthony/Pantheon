/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * $Id: DeleteTopicRequestHeader.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package com.pantheon.common.protocol.header;


import com.pantheon.remoting.CommandCustomHeader;
import com.pantheon.remoting.annotation.CFNotNull;
import com.pantheon.remoting.exception.RemotingCommandException;

public class GetServerNodeIdResponseHeader implements CommandCustomHeader {
    @CFNotNull
    private Integer serverNodeId;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public Integer getServerNodeId() {
        return serverNodeId;
    }

    public void setServerNodeId(Integer serverNodeId) {
        this.serverNodeId = serverNodeId;
    }
}