/*
 * Copyright © 2026 Lolaf.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lolaf.primos.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The durable store: a 24-byte record, fsync'd and replaced atomically.
 *
 * <p>Writing goes temp file, fsync, atomic rename, fsync the directory. A torn write cannot exist,
 * because the previous file stays intact until the rename succeeds. That costs two fsyncs where a
 * double-slot scheme would cost one, but elections are rare enough that the difference is
 * invisible and the simple thing is the one still obviously correct in a year.
 *
 * <p>A CRC covers the record, because an atomic rename protects against a torn write but not
 * against bit rot.
 *
 * <p><strong>Missing and corrupt are treated completely differently.</strong> A missing file is a
 * node that has never run. A file that fails its checksum makes the store refuse to open: falling
 * back to "fresh" there is exactly how a node forgets a vote and elects a second leader, so it is
 * a loud failure needing a human, never a recoverable warning.
 *
 * <p>An exclusive lock is held on a separate lock file for the store's lifetime. Two processes
 * sharing one state directory — a botched restart, a container that did not actually die — would
 * otherwise each vote independently from the same identity, which is a double vote by construction
 * and completely invisible from outside. The lock is on its own file rather than on the state file
 * because the state file is replaced by rename, so its identity changes under any lock held on it.
 *
 * <p>Note that {@link #save} blocks the node thread while the disk syncs. That is inherent: the
 * value must be durable before the node acts on it. Keep the directory on local storage and the
 * election timeout comfortably above worst-case sync latency.
 */
public final class FileStateStore implements StateStore {

    private static final Logger log = LoggerFactory.getLogger(FileStateStore.class);

    static final byte[] MAGIC = {'P', 'R', 'M', 'S'};
    static final byte VERSION = 1;

    /** magic(4) version(1) reserved(3) term(8) votedFor(4) crc(4) */
    static final int RECORD_BYTES = 24;

    private static final int OFFSET_TERM = 8;
    private static final int OFFSET_VOTED_FOR = 16;
    private static final int OFFSET_CRC = 20;

    private final Path directory;
    private final Path statePath;
    private final Path tempPath;
    private final Path lockPath;
    private final ByteBuffer record = ByteBuffer.allocate(RECORD_BYTES);
    private final CRC32 crc = new CRC32();

    private FileChannel lockChannel;
    private FileLock lock;
    private boolean closed;

    /**
     * Opens the store, taking its exclusive lock.
     *
     * @throws StateStoreException if the directory is unusable, another process already holds the
     *                             lock, or existing state is corrupt
     */
    public FileStateStore(Path directory) {
        this.directory = Objects.requireNonNull(directory, "directory");
        this.statePath = directory.resolve("state");
        this.tempPath = directory.resolve("state.tmp");
        this.lockPath = directory.resolve("state.lock");

        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new StateStoreException("cannot create state directory " + directory, e);
        }
        acquireLock();
    }

    @Override
    public PersistentState load() {
        ensureOpen();
        byte[] bytes;
        try {
            if (!Files.exists(statePath)) {
                return PersistentState.fresh();
            }
            bytes = Files.readAllBytes(statePath);
        } catch (IOException e) {
            throw new StateStoreException("cannot read " + statePath, e);
        }
        return decode(bytes);
    }

    @Override
    public void save(long currentTerm, int votedFor) {
        ensureOpen();
        encode(currentTerm, votedFor);

        try (FileChannel channel = FileChannel.open(tempPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            record.rewind();
            while (record.hasRemaining()) {
                channel.write(record);
            }
            channel.force(true);
        } catch (IOException e) {
            throw new StateStoreException("cannot write " + tempPath, e);
        }

        try {
            Files.move(tempPath, statePath,
                    StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new StateStoreException("cannot replace " + statePath, e);
        }
        syncDirectory();
    }

    @Override
    public boolean isDurable() {
        return true;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            if (lock != null) {
                lock.release();
            }
        } catch (IOException e) {
            log.debug("releasing the state lock failed", e);
        }
        try {
            if (lockChannel != null) {
                lockChannel.close();
            }
        } catch (IOException e) {
            log.debug("closing the state lock file failed", e);
        }
    }

    private void acquireLock() {
        try {
            lockChannel = FileChannel.open(lockPath,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            lock = lockChannel.tryLock();
        } catch (IOException e) {
            throw new StateStoreException("cannot open the state lock " + lockPath, e);
        } catch (OverlappingFileLockException e) {
            lock = null;
        }
        if (lock == null) {
            close();
            throw new StateStoreException(
                    "another process is already using the state directory " + directory
                            + ". Two nodes sharing one directory would vote independently from the"
                            + " same identity, which elects two leaders in one term.");
        }
    }

    /**
     * Makes the rename itself durable. Without this the file can be back after a crash but the
     * directory entry pointing at it need not be.
     *
     * <p>Best effort: some platforms, Windows among them, refuse to open a directory as a channel.
     * The rename is still atomic there, so the window this closes is narrower rather than absent.
     */
    private void syncDirectory() {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (IOException | UnsupportedOperationException e) {
            log.debug("this platform does not support syncing a directory: {}", e.toString());
        }
    }

    private void encode(long currentTerm, int votedFor) {
        record.clear();
        record.put(MAGIC);
        record.put(VERSION);
        record.put((byte) 0).put((byte) 0).put((byte) 0);
        record.putLong(currentTerm);
        record.putInt(votedFor);
        record.putInt(checksumOf(record.array()));
        record.flip();
    }

    private PersistentState decode(byte[] bytes) {
        if (bytes.length != RECORD_BYTES) {
            throw corrupt("expected " + RECORD_BYTES + " bytes but found " + bytes.length);
        }
        for (int i = 0; i < MAGIC.length; i++) {
            if (bytes[i] != MAGIC[i]) {
                throw corrupt("not a primos state file");
            }
        }
        if (bytes[4] != VERSION) {
            throw corrupt("state file version " + bytes[4] + ", this build writes " + VERSION);
        }
        ByteBuffer view = ByteBuffer.wrap(bytes);
        int expected = checksumOf(bytes);
        int found = view.getInt(OFFSET_CRC);
        if (expected != found) {
            throw corrupt("checksum mismatch");
        }
        return new PersistentState(view.getLong(OFFSET_TERM), view.getInt(OFFSET_VOTED_FOR));
    }

    /** Over everything up to, but not including, the checksum itself. */
    private int checksumOf(byte[] bytes) {
        crc.reset();
        crc.update(bytes, 0, OFFSET_CRC);
        return (int) crc.getValue();
    }

    private StateStoreException corrupt(String detail) {
        return new StateStoreException(
                "the state file " + statePath + " is corrupt (" + detail + "). Refusing to start:"
                        + " treating unreadable state as a fresh node would let this node vote"
                        + " twice in one term and elect a second leader. Investigate rather than"
                        + " delete, unless you are certain this node never voted.");
    }

    private void ensureOpen() {
        if (closed) {
            throw new StateStoreException("the state store for " + directory + " is closed");
        }
    }
}
