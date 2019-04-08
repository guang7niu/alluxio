/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.fuse;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;
import alluxio.util.ConfigurationUtils;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.SetAttributePOptions;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.AlluxioException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import jnr.ffi.Pointer;
import jnr.ffi.types.gid_t;
import jnr.ffi.types.mode_t;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;
import jnr.ffi.types.uid_t;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;
import ru.serce.jnrfuse.struct.Timespec;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.TimeoutException;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

import alluxio.grpc.CheckConsistencyPOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.client.file.FileSystemUtils;
import alluxio.MetaCache;
import alluxio.grpc.LoadMetadataPType;
import alluxio.retry.CountingRetry;
import java.util.concurrent.ConcurrentHashMap;
import alluxio.wire.MountPointInfo;

/**
 * Main FUSE implementation class.
 *
 * Implements the FUSE callbacks defined by jnr-fuse.
 */
@ThreadSafe
public final class AlluxioFuseFileSystem extends FuseStubFS {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioFuseFileSystem.class);
  private static final int MAX_OPEN_FILES = Integer.MAX_VALUE;
  private static final int MAX_OPEN_WAITTIME_MS = 5000;

  /**
   * 4294967295 is unsigned long -1, -1 means that uid or gid is not set.
   * 4294967295 or -1 occurs when chown without user name or group name.
   * Please view https://github.com/SerCeMan/jnr-fuse/issues/67 for more details.
   */
  @VisibleForTesting
  public static final long ID_NOT_SET_VALUE = -1;
  @VisibleForTesting
  public static final long ID_NOT_SET_VALUE_UNSIGNED = 4294967295L;

  private static final long UID = AlluxioFuseUtils.getUid(System.getProperty("user.name"));
  private static final long GID = AlluxioFuseUtils.getGid(System.getProperty("user.name"));

  // SM
  private static AlluxioConfiguration mConf = new InstancedConfiguration(ConfigurationUtils.defaults());
  private static final String mSeed = ".tmp.ava.alluxiosc.tmp";
  private static final String mCacheSeed = ".tmp.cache.tmp.ava.alluxiosc.tmp";
  private static final int SHORT_CIRCUIT_SIZE = 512;
  private static final String CUR_MARKER = "@marker current: ";
  private static final String RESET_MARKER = "@marker reset: ls @";

  class OpenFileEntry2 extends OpenFileEntry {
    private FuseFileInfo mFI;
    private boolean mCreated;

    public OpenFileEntry2(long id, String path, FileInStream in, FileOutStream out, FuseFileInfo fi) {
      super(id, path, in, out);
      mFI = fi;
      mCreated = false;
    }

    public FuseFileInfo getFI() {
      return mFI;
    }

    public void setFI(FuseFileInfo fi) {
      mFI = fi;
    }

    public boolean getCreated() {
      return mCreated;
    }

    public void setCreated(boolean created) {
      mCreated = created;
    }
  }

  // Open file managements
  private static final IndexDefinition<OpenFileEntry2, Long> ID_INDEX =
      new IndexDefinition<OpenFileEntry2, Long>(true) {
        @Override
        public Long getFieldValue(OpenFileEntry2 o) {
          return o.getId();
        }
      };

  private static final IndexDefinition<OpenFileEntry2, String> PATH_INDEX =
      new IndexDefinition<OpenFileEntry2, String>(true) {
        @Override
        public String getFieldValue(OpenFileEntry2 o) {
          return o.getPath();
        }
      };

  private final boolean mIsUserGroupTranslation;
  private final FileSystem mFileSystem;
  // base path within Alluxio namespace that is used for FUSE operations
  // For example, if alluxio-fuse is mounted in /mnt/alluxio and mAlluxioRootPath
  // is /users/foo, then an operation on /mnt/alluxio/bar will be translated on
  // an action on the URI alluxio://<master>:<port>/users/foo/bar
  private final Path mAlluxioRootPath;
  // Keeps a cache of the most recently translated paths from String to Alluxio URI
  //private final LoadingCache<String, AlluxioURI> mPathResolverCache;  // SM

  // Table of open files with corresponding InputStreams and OutputStreams
  private final IndexedSet<OpenFileEntry2> mOpenFiles;

  private long mNextOpenFileId;
  private final String mFsName;

  /**
   * Creates a new instance of {@link AlluxioFuseFileSystem}.
   *
   * @param fs Alluxio file system
   * @param opts options
   * @param conf Alluxio configuration
   */
  public AlluxioFuseFileSystem(FileSystem fs, AlluxioFuseOptions opts, AlluxioConfiguration conf) {
    super();
    mFsName = conf.get(PropertyKey.FUSE_FS_NAME);
    mFileSystem = fs;
    mAlluxioRootPath = Paths.get(opts.getAlluxioRoot());
    MetaCache.setAlluxioRootPath(mAlluxioRootPath); // call this as earlier as possible - SM
    mNextOpenFileId = 0L;
    mOpenFiles = new IndexedSet<>(ID_INDEX, PATH_INDEX);

    // SM
    //final int maxCachedPaths = conf.getInt(PropertyKey.FUSE_CACHED_PATHS_MAX);
    mIsUserGroupTranslation
        = conf.getBoolean(PropertyKey.FUSE_USER_GROUP_TRANSLATION_ENABLED);
    //mPathResolverCache = CacheBuilder.newBuilder()
    //    .maximumSize(maxCachedPaths)
    //    .build(new PathCacheLoader());

    Preconditions.checkArgument(mAlluxioRootPath.isAbsolute(),
        "alluxio root path should be absolute");
  }

  // SM
  public long getNextId() {
    synchronized (mOpenFiles) {
      return mNextOpenFileId++;
    }
  }

  /**
   * Changes the mode of an Alluxio file.
   *
   * @param path the path of the file
   * @param mode the mode to change to
   * @return 0 on success, a negative value on error
   */
  @Override
  public int chmod(String path, @mode_t long mode) {
    //AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    AlluxioURI uri = MetaCache.getURI(path);  // SM

    SetAttributePOptions options = SetAttributePOptions.newBuilder()
        .setMode(new alluxio.security.authorization.Mode((short) mode).toProto()).build();
    try {
      MetaCache.invalidate(path);  // SM
      mFileSystem.setAttribute(uri, options);
    } catch (Throwable t) {
      LOG.error("Failed to change {} to mode {}", path, mode, t);
      return AlluxioFuseUtils.getErrorCode(t);
    }

    return 0;
  }

  /**
   * Changes the user and group ownership of an Alluxio file.
   * This operation only works when the user group translation is enabled in Alluxio-FUSE.
   *
   * @param path the path of the file
   * @param uid the uid to change to
   * @param gid the gid to change to
   * @return 0 on success, a negative value on error
   */
  @Override
  public int chown(String path, @uid_t long uid, @gid_t long gid) {
    if (!mIsUserGroupTranslation) {
      LOG.info("Cannot change the owner/group of path {}. Please set {} to be true to enable "
          + "user group translation in Alluxio-FUSE.",
          path, PropertyKey.FUSE_USER_GROUP_TRANSLATION_ENABLED.getName());
      return -ErrorCodes.EOPNOTSUPP();
    }

    try {
      SetAttributePOptions.Builder optionsBuilder = SetAttributePOptions.newBuilder();
      final AlluxioURI uri = MetaCache.getURI(path);  // SM

      String userName = "";
      if (uid != ID_NOT_SET_VALUE && uid != ID_NOT_SET_VALUE_UNSIGNED) {
        userName = AlluxioFuseUtils.getUserName(uid);
        if (userName.isEmpty()) {
          // This should never be reached
          LOG.error("Failed to get user name from uid {}", uid);
          return -ErrorCodes.EINVAL();
        }
        optionsBuilder.setOwner(userName);
      }

      String groupName = "";
      if (gid != ID_NOT_SET_VALUE && gid != ID_NOT_SET_VALUE_UNSIGNED) {
        groupName = AlluxioFuseUtils.getGroupName(gid);
        if (groupName.isEmpty()) {
          // This should never be reached
          LOG.error("Failed to get group name from gid {}", gid);
          return -ErrorCodes.EINVAL();
        }
        optionsBuilder.setGroup(groupName);
      } else if (!userName.isEmpty()) {
        groupName = AlluxioFuseUtils.getGroupName(userName);
        optionsBuilder.setGroup(groupName);
      }

      if (userName.isEmpty() && groupName.isEmpty()) {
        // This should never be reached
        LOG.info("Unable to change owner and group of file {} when uid is {} and gid is {}", path,
            userName, groupName);
      } else if (userName.isEmpty()) {
        LOG.info("Change group of file {} to {}", path, groupName);
        MetaCache.invalidate(path); // SM
        mFileSystem.setAttribute(uri, optionsBuilder.build());
      } else {
        LOG.info("Change owner of file {} to {}", path, groupName);
        mFileSystem.setAttribute(uri, optionsBuilder.build());
      }
    } catch (Throwable t) {
      LOG.error("Failed to chown {} to uid {} and gid {}", path, uid, gid, t);
      return AlluxioFuseUtils.getErrorCode(t);
    }
    return 0;
  }

  /**
   * Creates and opens a new file.
   *
   * @param path The FS path of the file to open
   * @param mode mode flags
   * @param fi FileInfo data struct kept by FUSE
   * @return 0 on success. A negative value on error
   */
  @Override
  public int create(String path, @mode_t long mode, FuseFileInfo fi) {
    //final AlluxioURI uri = mPathResolverCache.getUnchecked(path);   // SM
    final AlluxioURI uri = MetaCache.getURI(path);
    final int flags = fi.flags.get();
    LOG.trace("create({}, {}) [Alluxio: {}]", path, Integer.toHexString(flags), uri);

    try {
      if (mOpenFiles.size() >= MAX_OPEN_FILES) {
        LOG.error("Cannot create {}: too many open files (MAX_OPEN_FILES: {})", path,
            MAX_OPEN_FILES);
        return -ErrorCodes.EMFILE();
      }

      FileOutStream os = mFileSystem.createFile(uri);
      synchronized (mOpenFiles) {
        OpenFileEntry2 oe = new OpenFileEntry2(mNextOpenFileId, path, null, os, fi);
        oe.setCreated(true);
        mOpenFiles.add(oe);
        fi.fh.set(mNextOpenFileId);

        // Assuming I will never wrap around (2^64 open files are quite a lot anyway)
        mNextOpenFileId += 1;
      }
      LOG.debug("{} created and opened", path);
    } catch (FileAlreadyExistsException e) {
      LOG.debug("Failed to create {}, file already exists", path);
      return -ErrorCodes.EEXIST();
    } catch (InvalidPathException e) {
      LOG.debug("Failed to create {}, path is invalid", path);
      return -ErrorCodes.ENOENT();
    } catch (Throwable t) {
      LOG.error("Failed to create {}", path, t);
      return AlluxioFuseUtils.getErrorCode(t);
    }

    return 0;
  }

  // SM
  private int flush_sc(String path, FuseFileInfo fi) {
    return 0;
  }

  /**
   * Flushes cached data on Alluxio.
   *
   * Called on explicit sync() operation or at close().
   *
   * @param path The path on the FS of the file to close
   * @param fi FileInfo data struct kept by FUSE
   * @return 0 on success, a negative value on error
   */
  @Override
  public int flush(String path, FuseFileInfo fi) {
    if (path.endsWith(mSeed)) {   // SM
      return flush_sc(path, fi);
    }
    LOG.trace("flush({})", path);
    final long fd = fi.fh.get();
    OpenFileEntry2 oe = mOpenFiles.getFirstByField(ID_INDEX, fd);
    if (oe == null) {
      LOG.error("Cannot find fd for {} in table", path);
      return -ErrorCodes.EBADFD();
    }
    if (oe.getOut() != null) {
      try {
        oe.getOut().flush();
      } catch (IOException e) {
        MetaCache.invalidate(path);   // SM
        LOG.error("Failed to flush {}", path, e);
        return -ErrorCodes.EIO();
      }
      MetaCache.invalidate(path);  // SM
    } else {
      LOG.debug("Not flushing: {} was not open for writing", path);
    }
    return 0;
  }

  // SM
  private int getattr_sc(String path, FileStat stat) {
    final long ctime_sec = System.currentTimeMillis() / 1000;
    final long ctime_nsec = (ctime_sec  % 1000) * 1000;

    stat.st_ctim.tv_sec.set(ctime_sec);
    stat.st_ctim.tv_nsec.set(ctime_nsec);
    stat.st_mtim.tv_sec.set(ctime_sec);
    stat.st_mtim.tv_nsec.set(ctime_nsec);

    stat.st_size.set(SHORT_CIRCUIT_SIZE);
    stat.st_uid.set(UID);
    stat.st_gid.set(GID);
    stat.st_mode.set(FileStat.S_IFREG);

    if (path.endsWith(mCacheSeed)) {
      path = path.substring(0, path.length() - mCacheSeed.length()); 
      mFileSystem.asyncCache(path);
    }

    return 0;
  }

  /**
   * Retrieves file attributes.
   *
   * @param path The path on the FS of the file
   * @param stat FUSE data structure to fill with file attrs
   * @return 0 on success, negative value on error
   */
  @Override
  public int getattr(String path, FileStat stat) {
    if (path.endsWith(mSeed)) {   // SM
      return getattr_sc(path, stat);
    }
    //final AlluxioURI turi = mPathResolverCache.getUnchecked(path);
    AlluxioURI turi = MetaCache.getURI(path);
    URIStatus status = null;
    LOG.trace("getattr({}) [Alluxio: {}]", path, turi);
    int idx = path.lastIndexOf("/@");   // SM
    if (idx >= 0) {
      String f = path.substring(idx + 2);
      if (f.startsWith(CUR_MARKER) || f.startsWith(RESET_MARKER)) {
        status = MetaCache.file2Status(f);
      } else {
        path = path.substring(0, idx);
        if (path.equals("")) path = "/";
        turi = MetaCache.getURI(path);
      }
    }

    try {
      //URIStatus status = mFileSystem.getStatus(turi); // SM
      if (status == null) status = mFileSystem.getStatus(turi);
      if (!status.isCompleted()) {
        // Always block waiting for file to be completed except when the file is writing
        // We do not want to block the writing process
        if (!mOpenFiles.contains(PATH_INDEX, path) && !waitForFileCompleted(turi)) {
          LOG.error("File {} is not completed", path);
        }
        status = mFileSystem.getStatus(turi);
      }
      long size = status.getLength();
      stat.st_size.set(size);

      // Sets block number to fulfill du command needs
      // `st_blksize` is ignored in `getattr` according to
      // https://github.com/libfuse/libfuse/blob/d4a7ba44b022e3b63fc215374d87ed9e930d9974/include/fuse.h#L302
      // According to http://man7.org/linux/man-pages/man2/stat.2.html,
      // `st_blocks` is the number of 512B blocks allocated
      stat.st_blocks.set((int) Math.ceil((double) size / 512));

      final long ctime_sec = status.getLastModificationTimeMs() / 1000;
      // Keeps only the "residual" nanoseconds not caputred in citme_sec
      final long ctime_nsec = (status.getLastModificationTimeMs() % 1000) * 1000;

      stat.st_ctim.tv_sec.set(ctime_sec);
      stat.st_ctim.tv_nsec.set(ctime_nsec);
      stat.st_mtim.tv_sec.set(ctime_sec);
      stat.st_mtim.tv_nsec.set(ctime_nsec);

      if (mIsUserGroupTranslation) {
        // Translate the file owner/group to unix uid/gid
        // Show as uid==-1 (nobody) if owner does not exist in unix
        // Show as gid==-1 (nogroup) if group does not exist in unix
        stat.st_uid.set(AlluxioFuseUtils.getUid(status.getOwner()));
        stat.st_gid.set(AlluxioFuseUtils.getGidFromGroupName(status.getGroup()));
      } else {
        stat.st_uid.set(UID);
        stat.st_gid.set(GID);
      }

      int mode = status.getMode();
      if (status.isFolder()) {
        mode |= FileStat.S_IFDIR;
      } else {
        mode |= FileStat.S_IFREG;
      }
      stat.st_mode.set(mode);
      stat.st_nlink.set(1);
    } catch (FileDoesNotExistException | InvalidPathException e) {
      LOG.debug("Failed to get info of {}, path does not exist or is invalid", path);
      return -ErrorCodes.ENOENT();
    } catch (Throwable t) {
      LOG.error("Failed to get info of {}", path, t);
      return AlluxioFuseUtils.getErrorCode(t);
    }

    return 0;
  }

  /**
   * @return Name of the file system
   */
  @Override
  public String getFSName() {
    return mFsName;
  }

  /**
   * Creates a new dir.
   *
   * @param path the path on the FS of the new dir
   * @param mode Dir creation flags (IGNORED)
   * @return 0 on success, a negative value on error
   */
  @Override
  public int mkdir(String path, @mode_t long mode) {
    //final AlluxioURI turi = mPathResolverCache.getUnchecked(path);  // SM
    final AlluxioURI turi = MetaCache.getURI(path);
    LOG.trace("mkdir({}) [Alluxio: {}]", path, turi);
    try {
      mFileSystem.createDirectory(turi);
    } catch (FileAlreadyExistsException e) {
      LOG.debug("Failed to create directory {}, directory already exists", path);
      return -ErrorCodes.EEXIST();
    } catch (InvalidPathException e) {
      LOG.debug("Failed to create directory {}, path is invalid", path);
      return -ErrorCodes.ENOENT();
    } catch (Throwable t) {
      LOG.error("Failed to create directory {}", path, t);
      return AlluxioFuseUtils.getErrorCode(t);
    }

    return 0;
  }

  // SM
  private int open_sc(String path, FuseFileInfo fi) {
    long id = getNextId();
    fi.fh.set(id);
    return 0;
  }

  /**
   * Opens an existing file for reading.
   *
   * Note that the opening an existing file would fail, because of Alluxio's write-once semantics.
   *
   * @param path the FS path of the file to open
   * @param fi FileInfo data structure kept by FUSE
   * @return 0 on success, a negative value on error
   */
  @Override
  public int open(String path, FuseFileInfo fi) {
    if (path.endsWith(mSeed)) {   // SM
      return open_sc(path, fi);
    }
    //final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    final AlluxioURI uri = MetaCache.getURI(path);
    // (see {@code man 2 open} for the structure of the flags bitfield)
    // File creation flags are the last two bits of flags
    final int flags = fi.flags.get();
    LOG.trace("open({}, 0x{}) [Alluxio: {}]", path, Integer.toHexString(flags), uri);

    try {
      final URIStatus status = mFileSystem.getStatus(uri);
      if (status.isFolder()) {
        LOG.error("Cannot open folder {}", path);
        return -ErrorCodes.EISDIR();
      }

      if (!status.isCompleted() && !waitForFileCompleted(uri)) {
        LOG.error("Cannot open incomplete folder {}", path);
        return -ErrorCodes.EFAULT();
      }

      if (mOpenFiles.size() >= MAX_OPEN_FILES) {
        LOG.error("Cannot open {}: too many open files (MAX_OPEN_FILES: {})", path, MAX_OPEN_FILES);
        return ErrorCodes.EMFILE();
      }

      FileInStream is = mFileSystem.openFile(uri);
      synchronized (mOpenFiles) {
        mOpenFiles.add(new OpenFileEntry2(mNextOpenFileId, path, is, null, fi));
        fi.fh.set(mNextOpenFileId);
        // Assuming I will never wrap around (2^64 open files are quite a lot anyway)
        mNextOpenFileId += 1;
      }
    } catch (FileDoesNotExistException | InvalidPathException e) {
      LOG.debug("Failed to open file {}, path does not exist or is invalid", path);
      return -ErrorCodes.ENOENT();
    } catch (Throwable t) {
      LOG.error("Failed to open file {}", path, t);
      return AlluxioFuseUtils.getErrorCode(t);
    }

    return 0;
  }

  /** // SM
   * FUSE all FileInStrea, which calls BlockInStream. 
   * If a block is evicted during file persisting, that is, file status changed from
   * NOT_PERSISTED to PERSISTED, then block may be disappeared and the block read can hit
   * a NOTFOUNDEXCEPTION (child of IOException). FileInStream already handles some retry for block,
   * and FUSE needs to handle retry for file, also.
   */
  private boolean handleRetryableException(String path, long fd, IOException e) {
      MetaCache.invalidate(path);
      MetaCache.invalidateWorkerInfoList();
      if (!(e instanceof NotFoundException)) return false;
      try {
        synchronized (mOpenFiles) {
          OpenFileEntry2 oe = mOpenFiles.getFirstByField(ID_INDEX, fd);
          if (oe != null) {
            mOpenFiles.remove(oe);
            oe.close();
          }
          oe = new OpenFileEntry2(fd, path, mFileSystem.openFile(MetaCache.getURI(path)), 
              null, (oe != null) ? oe.getFI() : null);
          mOpenFiles.add(oe);
        }
      } catch (IOException | AlluxioException e2) {
          LOG.error("=== get err when handle retry exception " + e2.getMessage());
          return false;
      }
      return true;
  }

  private int read_sc(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
    path = path.substring(0, path.length() - mSeed.length()); 
    String localPath = mFileSystem.acquireShortCircuitPath(path);
    LOG.debug("!!! sc local block: {} for {}", localPath, path);
    localPath = localPath + System.getProperty("line.separator");
    byte []dest = new byte[SHORT_CIRCUIT_SIZE];
    System.arraycopy(localPath.getBytes(), 0, dest, 0, localPath.length());
    buf.put(0, dest, 0, dest.length);
    return dest.length;
  }

  /**
   * Reads data from an open file.
   *
   * @param path the FS path of the file to read
   * @param buf FUSE buffer to fill with data read
   * @param size how many bytes to read. The maximum value that is accepted
   *             on this method is {@link Integer#MAX_VALUE} (note that current
   *             FUSE implementation will call this method with a size of
   *             at most 128K).
   * @param offset offset of the read operation
   * @param fi FileInfo data structure kept by FUSE
   * @return the number of bytes read or 0 on EOF. A negative
   *         value on error
   */
  @Override
  public int read(String path, Pointer buf, @size_t long size, @off_t long offset,
      FuseFileInfo fi) {
    if (path.endsWith(mSeed)) {   // SM
      return read_sc(path, buf, size, offset, fi);
    }

    if (size > Integer.MAX_VALUE) {
      LOG.error("Cannot read more than Integer.MAX_VALUE");
      return -ErrorCodes.EINVAL();
    }
    LOG.trace("read({}, {}, {})", path, size, offset);
    final int sz = (int) size;
    final long fd = fi.fh.get();
    OpenFileEntry2 oe = mOpenFiles.getFirstByField(ID_INDEX, fd);
    if (oe == null) {
      LOG.error("Cannot find fd for {} in table", path);
      MetaCache.invalidate(path);
      return -ErrorCodes.EBADFD();
    }

    int rd = 0;
    int nread = 0;
    if (oe.getIn() == null) {
      LOG.error("{} was not open for reading", path);
      return -ErrorCodes.EBADFD();
    }
    try {
      oe.getIn().seek(offset);
      final byte[] dest = new byte[sz];
      while (rd >= 0 && nread < size) {
        rd = oe.getIn().read(dest, nread, sz - nread);
        if (rd >= 0) {
          nread += rd;
        }
      }

      if (nread == -1) { // EOF
        nread = 0;
      } else if (nread > 0) {
        buf.put(0, dest, 0, nread);
      }
    } catch (Throwable t) {
      LOG.error("Failed to read file {}", path, t);
      MetaCache.invalidate(path);
      return AlluxioFuseUtils.getErrorCode(t);
    }

    return nread;
  }

  // SM
  private void doInvalidate(AlluxioURI uri) {
    try {
      final URIStatus status = mFileSystem.getStatus(uri);
      if (status.isFolder()) {
        MetaCache.invalidatePrefix(uri.getPath());
      } else {
        MetaCache.invalidate(uri.getPath());
      }
    } catch (Exception e) {
      LOG.error("==== error doInvalidate getStatus {} ", uri);
    }
  }

  /**
   * Checks the consistency of Alluxio metadata against the under storage for all files and
   * directories in a given subtree.
   *
   * @param path the root of the subtree to check
   * @param options method options
   * @return a list of inconsistent files and directories
   */
  private static List<AlluxioURI> checkConsistency(AlluxioURI path, CheckConsistencyPOptions options)
      throws IOException {
    FileSystemContext context = FileSystemContext.create(mConf);
    FileSystemMasterClient client = context.acquireMasterClient();
    try {
      return client.checkConsistency(path, options);
    } finally {
      context.releaseMasterClient(client);
    }
  }

  /**
   * Repair the inconsistent path by delete it in alluxio only, and refresh master metadata
   */
  private void doRefresh(AlluxioURI uri) throws IOException {
      doInvalidate(uri);

      List<AlluxioURI> inconsistentUris = checkConsistency(uri, 
          CheckConsistencyPOptions.getDefaultInstance());
      DeletePOptions delopt = DeletePOptions.newBuilder().setAlluxioOnly(true).build();
      Map<String, MountPointInfo> mountTable = null;
      try {
        mountTable = mFileSystem.getMountTable();
      } catch (IOException e) {
        LOG.error("we should nerver get here, " +
          "failed to get mount table in do refresh, err: {}", e);
        throw e;
      } catch (AlluxioException e) {
        LOG.error("we should nerver get here, " +
          "failed to get mount table in do refresh, err: {}", e);
        throw new IOException("alluxio exception with message: " + e.getMessage());
      }

      for (AlluxioURI u : inconsistentUris) {
        try {
          if (mountTable.containsKey(u.getPath())) {
            LOG.warn("mount point {} is identified inconsistent," +
              " but we cann't delete it from file system", u.getPath());
            continue;
          }
          LOG.debug("==== delete {} in alluxio only", u.getPath());
          mFileSystem.delete(u, delopt);
        } catch (Exception e) {
          LOG.error("==== error during rm {} ", u.getPath());
        }
      }
  }

  class LsOption {
    public String mMarker;
    boolean mQuick;
    public LsOption() {
      mMarker = null;
      mQuick = true;
    }
    public LsOption quick(boolean q) {
      mQuick = q;
      return this;
    }
    public boolean quick() {
      return mQuick;
    }
    public LsOption marker(String m) {
      mMarker = m;
      return this;
    }
    public String marker() {
      return mMarker;
    }
  }

  private ConcurrentHashMap<String, LsOption> mLsOptions = new ConcurrentHashMap<>();
  private void addLsOption(String path, LsOption option) {
    for (Map.Entry<String, LsOption> entry: mLsOptions.entrySet()) {
      if (mLsOptions.size() < 1000) break;
      mLsOptions.remove(entry.getKey());
    }
    mLsOptions.put(path, option);
  }

  /**
   * Reads the contents of a directory.
   *
   * @param path The FS path of the directory
   * @param buff The FUSE buffer to fill
   * @param filter FUSE filter
   * @param offset Ignored in alluxio-fuse
   * @param fi FileInfo data structure kept by FUSE
   * @return 0 on success, a negative value on error
   */
  @Override
  public int readdir(String path, Pointer buff, FuseFillDir filter,
      @off_t long offset, FuseFileInfo fi) {
    //final AlluxioURI turi = mPathResolverCache.getUnchecked(path);
    LOG.trace("readdir({})", path);
    try {
      /*
       * @l: switch between detail and quick mode
       * @: unset marker, aka, start from beginning
       * @{marker}: set marker
       * @f: consistency check
       */
      List<URIStatus> ls = new ArrayList<>();
      int idx = path.lastIndexOf("/@");   // SM
      if (idx >= 0) {
        String dbg_txt = path.substring(idx).replace("/@", "");
        path = path.substring(0, idx);
        if (path.equals("")) path = "/";
        if (dbg_txt.startsWith("@")) {  // debug
          MetaCache.debugMetaCache(dbg_txt.substring(1));
          ls.add(MetaCache.file2Status("check debug output in fuse.out"));
        } else if (dbg_txt.length() == 0) {  // unset marker
          LsOption option = mLsOptions.get(path);
          if (option != null && option.mMarker != null) {
            ls.add(MetaCache.file2Status("unset marker " + option.mMarker));
            option.mMarker = null;
          } else {
            ls.add(MetaCache.file2Status("unset marker"));
          }
        } else if (dbg_txt.equals("f")) {
          doRefresh(MetaCache.getURI(path));
          ls.add(MetaCache.file2Status("Done check consistency"));
        } else if (dbg_txt.equals("l")) {
          LsOption option = mLsOptions.get(path);
          if (option == null) {
            option = new LsOption().quick(false);
            addLsOption(path, option);
          } else {
            option.quick(!option.quick());
          }
          if (option.quick()) {
            ls.add(MetaCache.file2Status("ls fast mode."));
          } else {
            ls.add(MetaCache.file2Status("ls detail mode."));
            MetaCache.invalidatePrefix(path);
          }
        } else if (dbg_txt.length() > 0) {
          LsOption option = mLsOptions.get(path);
          if (option == null) {
            option = new LsOption();
            addLsOption(path, option);
          }
          option.marker(dbg_txt);
          ls.add(MetaCache.file2Status("set marker " + dbg_txt));
        } else {
          ls.add(MetaCache.file2Status("invalid option."));
        }
      } else { // SM
        ListStatusPOptions.Builder builder = ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS);
        LsOption lop = mLsOptions.get(path);
        URIStatus curMarker = null, resetMarker = null;
        if (lop != null && lop.marker() != null) {
          builder.setMarker(lop.marker());
          curMarker = MetaCache.file2Status(CUR_MARKER + lop.marker());
          resetMarker = MetaCache.file2Status(RESET_MARKER);
        }
        if (lop != null && !lop.quick()) builder.setLoadMetadataType(LoadMetadataPType.ALWAYS);
        ls = mFileSystem.listStatus(MetaCache.getURI(path), builder.build());
        if (curMarker != null) ls.add(curMarker);
        if (resetMarker != null) ls.add(resetMarker);
      }

      // standard . and .. entries
      filter.apply(buff, ".", null, 0);
      filter.apply(buff, "..", null, 0);

      for (final URIStatus file : ls) {
        filter.apply(buff, file.getName(), null, 0);
      }
    } catch (FileDoesNotExistException | InvalidPathException e) {
      LOG.debug("Failed to read directory {}, path does not exist or is invalid", path);
      return -ErrorCodes.ENOENT();
    } catch (Throwable t) {
      LOG.error("Failed to read directory {}", path, t);
      return AlluxioFuseUtils.getErrorCode(t);
    }

    return 0;
  }

  // SM
  private int release_sc(String path, FuseFileInfo fi) {
    return 0;
  }

  /**
   * Releases the resources associated to an open file. Release() is async.
   *
   * Guaranteed to be called once for each open() or create().
   *
   * @param path the FS path of the file to release
   * @param fi FileInfo data structure kept by FUSE
   * @return 0. The return value is ignored by FUSE (any error should be reported
   *         on flush instead)
   */
  @Override
  public int release(String path, FuseFileInfo fi) {
    if (path.endsWith(mSeed)) {   // SM
      return release_sc(path, fi);
    }
    LOG.trace("release({})", path);
    OpenFileEntry2 oe;
    final long fd = fi.fh.get();
    synchronized (mOpenFiles) {
      oe = mOpenFiles.getFirstByField(ID_INDEX, fd);
      mOpenFiles.remove(oe);
    }
    if (oe == null) {
      LOG.error("Cannot find fd for {} in table", path);
      return -ErrorCodes.EBADFD();
    }

    if (oe.getCreated()) MetaCache.invalidate(path);  // SM

    try {
      oe.close();
    } catch (IOException e) {
      LOG.error("Failed closing {} [in]", path, e);
    }
    return 0;
  }

  /**
   * Renames a path.
   *
   * @param oldPath the source path in the FS
   * @param newPath the destination path in the FS
   * @return 0 on success, a negative value on error
   */
  @Override
  public int rename(String oldPath, String newPath) {
    // SM
    //final AlluxioURI oldUri = mPathResolverCache.getUnchecked(oldPath);
    //final AlluxioURI newUri = mPathResolverCache.getUnchecked(newPath);
    final AlluxioURI oldUri = MetaCache.getURI(oldPath);
    final AlluxioURI newUri = MetaCache.getURI(newPath);
    LOG.trace("rename({}, {}) [Alluxio: {}, {}]", oldPath, newPath, oldUri, newUri);

    try {
      if (!mFileSystem.exists(oldUri)) {
        LOG.error("File {} does not exist", oldPath);
        return -ErrorCodes.ENOENT();
      }
      if (mFileSystem.exists(newUri)) {
        LOG.error("File {} already exists, please delete the destination file first", newPath);
        return -ErrorCodes.EEXIST();
      }
      doInvalidate(oldUri);
      doInvalidate(newUri);
      mFileSystem.rename(oldUri, newUri);
    } catch (FileDoesNotExistException e) {
      LOG.debug("Failed to rename {} to {}, file {} does not exist", oldPath, newPath, oldPath);
      return -ErrorCodes.ENOENT();
    } catch (FileAlreadyExistsException e) {
      LOG.debug("Failed to rename {} to {}, file {} already exists", oldPath, newPath, newPath);
      return -ErrorCodes.EEXIST();
    } catch (Throwable t) {
      LOG.error("Failed to rename {} to {}", oldPath, newPath, t);
      return AlluxioFuseUtils.getErrorCode(t);
    }

    return 0;
  }

  /**
   * Deletes an empty directory.
   *
   * @param path The FS path of the directory
   * @return 0 on success, a negative value on error
   */
  @Override
  public int rmdir(String path) {
    LOG.trace("rmdir({})", path);
    return rmInternal(path);
  }

  /**
   * Changes the size of a file. This operation would not succeed because of Alluxio's write-once
   * model.
   */
  @Override
  public int truncate(String path, long size) {
    //final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    final AlluxioURI uri = MetaCache.getURI(path);  // SM
    MetaCache.invalidate(path);
    LOG.debug("========== truncate {} size {}", path, size);
    try {
      if (!mFileSystem.exists(uri)) {
        LOG.error("File {} does not exist", uri);
        return -ErrorCodes.ENOENT();
      }
      OpenFileEntry2 oe = mOpenFiles.getFirstByField(PATH_INDEX, path);
      if (0 == size) {
        try {
          unlink(path); 
        } catch (Exception e) {
          LOG.warn("=== exception during unlink {} in trunk {}", path, e.getMessage());
        }
        synchronized (mOpenFiles) {
          try {
            if (oe != null) {
              mOpenFiles.remove(oe);
              oe.close();
              oe = new OpenFileEntry2(oe.getId(), path, null, mFileSystem.createFile(uri), oe.getFI());
            } else {
              oe = new OpenFileEntry2(mNextOpenFileId++, path, null, mFileSystem.createFile(uri), null);
            }
            mOpenFiles.add(oe);
          } catch (Exception e) {
            LOG.warn("=== exception during truncate create {}: {}", path, e.getMessage());
          }
        }
        return 0;
      } 
      if (oe == null || !oe.getCreated() || oe.getOut() == null || size < oe.getWriteOffset()) {
        LOG.error("File {} does not exist or not open for write or size {} is too small", uri, size);
        return -ErrorCodes.EINVAL();
      }
      int mb = 1024 * 1024;
      byte[] bz = new byte[mb];
      while (oe.getWriteOffset() < size) {
        if (oe.getWriteOffset() + mb < size) {
          oe.getOut().write(bz);
          oe.setWriteOffset(oe.getWriteOffset() + mb);
        } else {
          oe.getOut().write(bz, 0, (int)(size - oe.getWriteOffset()));
          oe.setWriteOffset(size);
        }
      }
      return 0;
    } catch (IOException e) {
      LOG.error("IOException encountered at path {}", path, e);
      return -ErrorCodes.EIO();
    } catch (AlluxioException e) {
      LOG.error("AlluxioException encountered at path {}", path, e);
      return -ErrorCodes.EFAULT();
    } catch (Throwable e) {
      LOG.error("Unexpected exception at path {}", path, e);
      return -ErrorCodes.EFAULT();
    }
    //LOG.error("File {} exists and cannot be overwritten. Please delete the file first", uri);
    //return -ErrorCodes.EEXIST();
  }

  /**
   * Deletes a file from the FS.
   *
   * @param path the FS path of the file
   * @return 0 on success, a negative value on error
   */
  @Override
  public int unlink(String path) {
    LOG.trace("unlink({})", path);
    return rmInternal(path);
  }

  /**
   * Alluxio does not have access time, and the file is created only once. So this operation is a
   * no-op.
   */
  @Override
  public int utimens(String path, Timespec[] timespec) {
    return 0;
  }

  /**
   * Writes a buffer to an open Alluxio file. Random write is not supported, so the offset argument
   * is ignored. Also, due to an issue in OSXFUSE that may write the same content at a offset
   * multiple times, the write also checks that the subsequent write of the same offset is ignored.
   *
   * @param buf The buffer with source data
   * @param size How much data to write from the buffer. The maximum accepted size for writes is
   *        {@link Integer#MAX_VALUE}. Note that current FUSE implementation will anyway call write
   *        with at most 128K writes
   * @param offset The offset where to write in the file (IGNORED)
   * @param fi FileInfo data structure kept by FUSE
   * @return number of bytes written on success, a negative value on error
   */
  @Override
  public int write(String path, Pointer buf, @size_t long size, @off_t long offset,
                   FuseFileInfo fi) {
    if (size > Integer.MAX_VALUE) {
      LOG.error("Cannot write more than Integer.MAX_VALUE");
      return ErrorCodes.EIO();
    }
    LOG.trace("write({}, {}, {})", path, size, offset);
    final int sz = (int) size;
    final long fd = fi.fh.get();
    OpenFileEntry2 oe = mOpenFiles.getFirstByField(ID_INDEX, fd);
    if (oe == null) {
      LOG.error("Cannot find fd for {} in table", path);
      return -ErrorCodes.EBADFD();
    }

    if (oe.getOut() == null) {
      LOG.error("{} already exists in Alluxio and cannot be overwritten."
          + " Please delete this file first.", path);
      return -ErrorCodes.EEXIST();
    }

    if (offset < oe.getWriteOffset()) {
      // no op
      return sz;
    }

    try {
      final byte[] dest = new byte[sz];
      buf.get(0, dest, 0, sz);
      oe.getOut().write(dest);
      oe.setWriteOffset(offset + size);
    } catch (IOException e) {
      LOG.error("IOException while writing to {}.", path, e);
      return -ErrorCodes.EIO();
    } finally {
      MetaCache.invalidate(path);   // SM
    }

    return sz;
  }

  /**
   * Convenience internal method to remove files or non-empty directories.
   *
   * @param path The path to remove
   * @return 0 on success, a negative value on error
   */
  private int rmInternal(String path) {
    //final AlluxioURI turi = mPathResolverCache.getUnchecked(path);
    final AlluxioURI turi = MetaCache.getURI(path);   // SM

    try {
      doInvalidate(turi);   // SM
      mFileSystem.delete(turi);
    } catch (FileDoesNotExistException | InvalidPathException e) {
      LOG.debug("Failed to remove {}, file does not exist or is invalid", path);
      return -ErrorCodes.ENOENT();
    } catch (Throwable t) {
      LOG.error("Failed to remove {}", path, t);
      return AlluxioFuseUtils.getErrorCode(t);
    }

    return 0;
  }

  /**
   * Waits for the file to complete before opening it.
   *
   * @param uri the file path to check
   * @return whether the file is completed or not
   */
  private boolean waitForFileCompleted(AlluxioURI uri) {
    try {
      CommonUtils.waitFor("file completed", () -> {
        try {
          return mFileSystem.getStatus(uri).isCompleted();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }, WaitForOptions.defaults().setTimeoutMs(MAX_OPEN_WAITTIME_MS));
      return true;
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      return false;
    } catch (TimeoutException te) {
      return false;
    }
  }

  /**
   * Exposed for testing.
   */
  LoadingCache<String, AlluxioURI> getPathResolverCache() {
    //return mPathResolverCache; // SM
    final int maxCachedPaths = mConf.getInt(PropertyKey.FUSE_CACHED_PATHS_MAX);
    LoadingCache<String, AlluxioURI> c = CacheBuilder.newBuilder()
        .maximumSize(maxCachedPaths)
        .build(new PathCacheLoader());
    return c;
  }

  /**
   * Resolves a FUSE path into {@link AlluxioURI} and possibly keeps it in the cache.
   */
  private final class PathCacheLoader extends CacheLoader<String, AlluxioURI> {

    /**
     * Constructs a new {@link PathCacheLoader}.
     */
    public PathCacheLoader() {}

    @Override
    public AlluxioURI load(String fusePath) {
      // fusePath is guaranteed to always be an absolute path (i.e., starts
      // with a fwd slash) - relative to the FUSE mount point
      final String relPath = fusePath.substring(1);
      final Path tpath = mAlluxioRootPath.resolve(relPath);

      return new AlluxioURI(tpath.toString());
    }
  }
}
