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

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dropwizard.http2.Http2CConnectorFactory;
import javax.annotation.Nullable;
import org.eclipse.jetty.http2.api.server.ServerSessionListener;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.thread.ThreadPool;

@JsonTypeName("h2cListener")
public class Http2ConnectorFactoryWithListener extends Http2CConnectorFactory
{

    private final ServerSessionListener listener;

    public Http2ConnectorFactoryWithListener(ServerSessionListener listener)
    {
        this.listener = listener;
    }

    @Override
    public Connector build(Server server, MetricRegistry metrics, String name, @Nullable ThreadPool threadPool)
    {
        Connector connector = super.build(server, metrics, name, threadPool);

        if (listener != null)
        {
            connector.addBean(listener);
        }
        return connector;
    }
}