// Copyright 2023 Goldman Sachs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.finos.legend.engine.server.core.session;

import org.eclipse.jetty.http2.api.Session;
import org.eclipse.jetty.http2.api.server.ServerSessionListener;
import org.eclipse.jetty.http2.frames.GoAwayFrame;
import org.eclipse.jetty.http2.frames.ResetFrame;

public class SessionListenerWithCancel extends ServerSessionListener.Adapter implements Session.Listener
{

    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger("SessionListenerWithCancel");


    public void onReset(Session session, ResetFrame resetFrame)
    {
        LOGGER.info("reset");
    }

    @Override
    public void onClose(Session session, GoAwayFrame goAwayFrame)
    {
        LOGGER.info("close");

    }

    @Override
    public void onFailure(Session session, Throwable throwable)
    {
        LOGGER.info("onFailure");

    }

}