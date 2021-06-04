/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.file.src;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.shaded.guava18.com.google.common.primitives.Longs;

/** A serializer for the {@link PendingSplitsCheckpoint}. */
@PublicEvolving
public final class PendingSplitsCheckpointSerializer<T extends FileSourceSplit>
        implements SimpleVersionedSerializer<PendingSplitsCheckpoint<T>> {

    private static final int VERSION = 2;

    private static final int VERSION_1_MAGIC_NUMBER = 0xDEADBEEF;
    private static final int VERSION_2_MAGIC_NUMBER = 0xDECAF147;

    private final SimpleVersionedSerializer<T> splitSerializer;

    public PendingSplitsCheckpointSerializer(SimpleVersionedSerializer<T> splitSerializer) {
        this.splitSerializer = checkNotNull(splitSerializer);
    }

    // ------------------------------------------------------------------------

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(PendingSplitsCheckpoint<T> checkpoint) throws IOException {
        checkArgument(
                checkpoint.getClass() == PendingSplitsCheckpoint.class,
                "Cannot serialize subclasses of PendingSplitsCheckpoint");

        // optimization: the splits lazily cache their own serialized form
        if (checkpoint.serializedFormCache != null) {
            return checkpoint.serializedFormCache;
        }

        final SimpleVersionedSerializer<T> splitSerializer = this.splitSerializer; // stack cache
        final Collection<T> splits = checkpoint.getSplits();
        final Map<Path, Long> processedPaths = checkpoint.getAlreadyProcessedPaths();
        final Long fileWatermark = checkpoint.getFileWatermark();

        final ArrayList<byte[]> serializedSplits = new ArrayList<>(splits.size());
        final HashMap<byte[], byte[]> serializedPaths = new HashMap<>(processedPaths.size());

        int totalLen =
                24; // four ints: magic, version of split serializer, count splits, count paths,
        // watemark

        for (T split : splits) {
            final byte[] serSplit = splitSerializer.serialize(split);
            serializedSplits.add(serSplit);
            totalLen += serSplit.length + 4; // 4 bytes for the length field
        }

        for (Map.Entry<Path, Long> entry : processedPaths.entrySet()) {
            final byte[] serPath = entry.getKey().toString().getBytes(StandardCharsets.UTF_8);
            final byte[] serWatemar = Longs.toByteArray(entry.getValue());
            serializedPaths.put(serPath, serWatemar);
            totalLen += serPath.length + serWatemar.length + 4 + 4; // 4 bytes for the length field
        }

        final byte[] result = new byte[totalLen];
        final ByteBuffer byteBuffer = ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.putInt(VERSION_2_MAGIC_NUMBER);
        byteBuffer.putInt(splitSerializer.getVersion());
        byteBuffer.putInt(serializedSplits.size());
        byteBuffer.putInt(serializedPaths.size());

        for (byte[] splitBytes : serializedSplits) {
            byteBuffer.putInt(splitBytes.length);
            byteBuffer.put(splitBytes);
        }

        for (Map.Entry<byte[], byte[]> entry : serializedPaths.entrySet()) {
            byteBuffer.putInt(entry.getKey().length);
            byteBuffer.put(entry.getKey());
            byteBuffer.putInt(entry.getValue().length);
            byteBuffer.put(entry.getValue());
        }

        byteBuffer.putLong(fileWatermark);

        assert byteBuffer.remaining() == 0;

        // optimization: cache the serialized from, so we avoid the byte work during repeated
        // serialization
        checkpoint.serializedFormCache = result;

        return result;
    }

    @Override
    public PendingSplitsCheckpoint<T> deserialize(int version, byte[] serialized)
            throws IOException {
        if (version == 1) {
            return deserializeV1(serialized);
        } else if (version == 2) {
            return deserializeV2(serialized);
        }
        throw new IOException("Unknown version: " + version);
    }

    private PendingSplitsCheckpoint<T> deserializeV1(byte[] serialized) throws IOException {
        final ByteBuffer bb = ByteBuffer.wrap(serialized).order(ByteOrder.LITTLE_ENDIAN);

        final int magic = bb.getInt();
        if (magic != VERSION_1_MAGIC_NUMBER) {
            throw new IOException(
                    String.format(
                            "Invalid magic number for PendingSplitsCheckpoint. "
                                    + "Expected: %X , found %X",
                            VERSION_1_MAGIC_NUMBER, magic));
        }

        final int splitSerializerVersion = bb.getInt();
        final int numSplits = bb.getInt();
        final int numPaths = bb.getInt();

        final SimpleVersionedSerializer<T> splitSerializer = this.splitSerializer; // stack cache
        final ArrayList<T> splits = new ArrayList<>(numSplits);
        final ArrayList<Path> paths = new ArrayList<>(numPaths);

        for (int remaining = numSplits; remaining > 0; remaining--) {
            final byte[] bytes = new byte[bb.getInt()];
            bb.get(bytes);
            final T split = splitSerializer.deserialize(splitSerializerVersion, bytes);
            splits.add(split);
        }

        for (int remaining = numPaths; remaining > 0; remaining--) {
            final byte[] bytes = new byte[bb.getInt()];
            bb.get(bytes);
            final Path path = new Path(new String(bytes, StandardCharsets.UTF_8));
            paths.add(path);
        }

        return PendingSplitsCheckpoint.reusingCollection(splits, paths);
    }

    private PendingSplitsCheckpoint<T> deserializeV2(byte[] serialized) throws IOException {
        final ByteBuffer bb = ByteBuffer.wrap(serialized).order(ByteOrder.LITTLE_ENDIAN);

        final int magic = bb.getInt();
        if (magic != VERSION_2_MAGIC_NUMBER) {
            throw new IOException(
                    String.format(
                            "Invalid magic number for PendingSplitsCheckpoint. "
                                    + "Expected: %X , found %X",
                            VERSION_2_MAGIC_NUMBER, magic));
        }

        final int splitSerializerVersion = bb.getInt();
        final int numSplits = bb.getInt();
        final int numPaths = bb.getInt();

        final SimpleVersionedSerializer<T> splitSerializer = this.splitSerializer; // stack cache
        final ArrayList<T> splits = new ArrayList<>(numSplits);
        final HashMap<Path, Long> paths = new HashMap<>(numPaths);

        for (int remaining = numSplits; remaining > 0; remaining--) {
            final byte[] bytes = new byte[bb.getInt()];
            bb.get(bytes);
            final T split = splitSerializer.deserialize(splitSerializerVersion, bytes);
            splits.add(split);
        }

        for (int remaining = numPaths; remaining > 0; remaining--) {
            final byte[] pathBytes = new byte[bb.getInt()];
            bb.get(pathBytes);
            final Path path = new Path(new String(pathBytes, StandardCharsets.UTF_8));
            final byte[] watermarkBytes = new byte[bb.getInt()];
            bb.get(watermarkBytes);
            long watermark = Longs.fromByteArray(watermarkBytes);
            paths.put(path, watermark);
        }

        long watermark = bb.getLong();

        return PendingSplitsCheckpoint.reusingCollection(splits, paths, watermark);
    }
}
