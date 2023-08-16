/*
 * //  Copyright 2023 Goldman Sachs
 * //
 * //  Licensed under the Apache License, Version 2.0 (the "License");
 * //  you may not use this file except in compliance with the License.
 * //  You may obtain a copy of the License at
 * //
 * //       http://www.apache.org/licenses/LICENSE-2.0
 * //
 * //  Unless required by applicable law or agreed to in writing, software
 * //  distributed under the License is distributed on an "AS IS" BASIS,
 * //  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * //  See the License for the specific language governing permissions and
 * //  limitations under the License.
 */

package org.finos.legend.engine.external.format.arrow;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.sql.SQLException;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;
import org.apache.arrow.adapter.jdbc.ArrowVectorIterator;
import org.apache.arrow.adapter.jdbc.JdbcToArrow;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfig;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfigBuilder;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.util.ValueVectorUtility;
import org.eclipse.collections.api.list.MutableList;
import org.finos.legend.engine.external.shared.runtime.ExternalFormatRuntimeExtension;
import org.finos.legend.engine.external.shared.runtime.write.ExternalFormatSerializeResult;
import org.finos.legend.engine.plan.dependencies.store.relational.IRelationalResult;
import org.finos.legend.engine.plan.dependencies.store.shared.IExecutionNodeContext;
import org.finos.legend.engine.plan.execution.nodes.helpers.ExecutionNodeSerializerHelper;
import org.finos.legend.engine.plan.execution.nodes.helpers.platform.DefaultExecutionNodeContext;
import org.finos.legend.engine.plan.execution.nodes.helpers.platform.ExecutionNodeJavaPlatformHelper;
import org.finos.legend.engine.plan.execution.nodes.helpers.platform.JavaHelper;
import org.finos.legend.engine.plan.execution.nodes.state.ExecutionState;
import org.finos.legend.engine.plan.execution.result.Result;
import org.finos.legend.engine.plan.execution.result.object.StreamingObjectResult;
import org.finos.legend.engine.plan.execution.stores.inMemory.plugin.StoreStreamReadingObjectsIterator;
import org.finos.legend.engine.plan.execution.stores.relational.result.RelationalResult;
import org.finos.legend.engine.protocol.pure.v1.model.executionPlan.nodes.JavaPlatformImplementation;
import org.finos.legend.engine.protocol.pure.v1.model.executionPlan.nodes.externalFormat.ExternalFormatExternalizeExecutionNode;
import org.finos.legend.engine.protocol.pure.v1.model.executionPlan.nodes.externalFormat.ExternalFormatInternalizeExecutionNode;
import org.pac4j.core.profile.CommonProfile;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ArrowRuntimeExtension implements ExternalFormatRuntimeExtension
{
    private static final String CONTENT_TYPE = "application/x.avro";

    @Override
    public List<String> getContentTypes()
    {
        return Collections.singletonList(CONTENT_TYPE);
    }

    @Override
    public Result executeExternalizeExecutionNode(ExternalFormatExternalizeExecutionNode node, Result result, MutableList<CommonProfile> profiles, ExecutionState executionState)
    {
        try
        {
            if (!(node.implementation instanceof JavaPlatformImplementation))
            {
                throw new RuntimeException("Only Java implementations are currently supported, found: " + node.implementation);
            }

            if (result instanceof  RelationalResult)
            {
                return  streamArrowFromRelational((RelationalResult) result);
            }
            else
            {            throw new RuntimeException("Arrow external format only supported on relational exeution");

            }

        }
          catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private Result streamArrowFromRelational(RelationalResult relationalResult) throws SQLException, IOException
    {
        BufferAllocator allocator = new RootAllocator();

        ArrowVectorIterator iterator = JdbcToArrow.sqlToArrowVectorIterator(relationalResult.getResultSet(), allocator);
        return new ExternalFormatSerializeResult(new ArrowDataWriter(iterator), relationalResult);
    }



}