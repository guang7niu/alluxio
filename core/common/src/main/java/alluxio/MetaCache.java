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

package alluxio;

import alluxio.util.ConfigurationUtils;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.client.file.URIStatus;
import alluxio.wire.FileInfo;
import alluxio.wire.BlockInfo;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.WorkerInfo;
import alluxio.wire.BlockLocation;
import alluxio.collections.ConcurrentHashSet;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import static java.util.stream.Collectors.toList;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the file meta cache
 */
@ThreadSafe
public class MetaCache {
  private static final Logger LOG = LoggerFactory.getLogger(MetaCache.class);

  private static Path alluxioRootPath = null;
  private static AlluxioConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());
  private static int maxCachedPaths = conf.getInt(PropertyKey.FUSE_CACHED_PATHS_MAX);
  private static LoadingCache<String, MetaCacheData> fcache 
    = CacheBuilder.newBuilder().maximumSize(maxCachedPaths).build(new MetaCacheLoader());
  private static LoadingCache<Long, BlockInfoData> bcache 
    = CacheBuilder.newBuilder().maximumSize(maxCachedPaths).build(new BlockInfoLoader());
  private static List<WorkerInfo> workerList = new ArrayList<>();
  private static AtomicLong lastWorkerListAccess = new AtomicLong(System.currentTimeMillis());
  private static boolean attrCacheEnabled = true;
  private static boolean blockCacheEnabled = true;
  private static boolean workerCacheEnabled = true;

  public static final int WORKER_PORT_MIN = 50000;
  public static final String WORKER_LOCAL_PATH = conf.get(PropertyKey.WORKER_LOCAL_PATH);
  public static final long LS_MAX_ENTRY = conf.getLong(PropertyKey.LS_MAX_ENTRY);

  public static final int ACTION_ASYNC_CACHE = 0;
  public static final int ACTION_X_CACHE = -1;
  public static final int ACTION_X_CACHE_REMOTE_EXISTED = -2;

  //qiniu worker -> array of files
  final static public int EVICT_EVICT = 0;
  final static public int EVICT_FREE = 1;
  static private Map <Long, Set<Long> > mEvictEvict = new ConcurrentHashMap<>();
  static private Map <Long, Set<Long> >mEvictFree = new ConcurrentHashMap<>();

  // need to synchronized
  static public Set<Long> getEvictBlock(int type, long worker) {
      Map <Long, Set<Long> > m = (EVICT_EVICT == type) ? mEvictEvict : mEvictFree;
      Set<Long> s = m.get(worker);
      if (s == null) {
          s = new ConcurrentHashSet<Long>();
          m.put(worker, s);
      }
      return s;
  }

  // add a place-hold PersistFile first, add blocks later
  static public boolean addEvictBlock(int type, long worker, long block) {
      Set<Long> s = getEvictBlock(type, worker);
      s.add(block);
      return true;
  }

  public static void setAlluxioRootPath(Path path) {
    alluxioRootPath = path;
  }

  public static void debugMetaCache(String p) {
    if (p.startsWith("a0")) {
      MetaCache.setAttrCache(0);
      System.out.println("Alluxio attr cache disabled.");
    } else if (p.startsWith("a1")) {
      MetaCache.setAttrCache(1);
      System.out.println("Alluxio attr cache enabled.");
    } else if (p.startsWith("ap")) {
      MetaCache.setAttrCache(2);
      System.out.println("Alluxio attr cache purged.");
    } else if (p.startsWith("as")) {
      System.out.println("Alluxio attr cache state:" + attrCacheEnabled);
      System.out.println("Alluxio attr cache size:" + fcache.size());
    } else if (p.startsWith("ac")) {
      p = p.substring(2);
      System.out.println("Attr cache for " + p + ":" + MetaCache.getStatus(MetaCache.resolve(p)));
    } else if (p.startsWith("b0")) {
      MetaCache.setBlockCache(0);
      System.out.println("Alluxio block cache disabled.");
    } else if (p.startsWith("b1")) {
      MetaCache.setBlockCache(1);
      System.out.println("Alluxio block cache enabled.");
    } else if (p.startsWith("bp")) {
      MetaCache.setBlockCache(2);
      System.out.println("Alluxio block cache purged.");
    } else if (p.startsWith("bs")) {
      System.out.println("Alluxio block cache state:" + blockCacheEnabled);
      System.out.println("Alluxio block cache size:" + bcache.size());
    } else if (p.startsWith("bc")) {
      p = p.substring(2);
      Long l = Long.parseLong(p);
      System.out.println("Cached block for " + l + ":" + MetaCache.getBlockInfo(l));
    } else if (p.startsWith("w0")) {
      MetaCache.setWorkerCache(0);
      System.out.println("Alluxio worker cache disabled.");
    } else if (p.startsWith("w1")) {
      MetaCache.setWorkerCache(1);
      System.out.println("Alluxio worker cache enabled.");
    } else if (p.startsWith("wp")) {
      MetaCache.setWorkerCache(2);
      System.out.println("Alluxio worker cache purged.");
    } else if (p.startsWith("ws")) {
      System.out.println("Cached workers state:" + workerCacheEnabled);
      System.out.println("Cached workers:" + MetaCache.getWorkerInfoList());
    }
  }

  public static void setAttrCache(int v) {
    switch (v) {
      case 0: 
        attrCacheEnabled = false; 
        fcache.invalidateAll();
        mWeight.set(0);
        mDirMap.clear();
        return;
      case 1: 
        attrCacheEnabled = true; 
        return;
      default: 
        fcache.invalidateAll(); 
    }
  }

  public static void setBlockCache(int v) {
    switch (v) {
      case 0: 
        blockCacheEnabled = false;
        bcache.invalidateAll();
        return;
      case 1:
        blockCacheEnabled = true;
        return;
      default:
        bcache.invalidateAll();
    }
  }

  public static void setWorkerCache(int v) {
    workerCacheEnabled = (0 == v) ? false : true;
    if (v > 1) MetaCache.invalidateWorkerInfoList();
  }

  public static void setWorkerInfoList(List<WorkerInfo> list) {
    if (!workerCacheEnabled) return;

    if (list != null) {
      synchronized (workerList) {
        workerList.clear();
        workerList.addAll(list);
      }
    }
  }

  public static List<WorkerInfo> getWorkerInfoList() {
    long now = System.currentTimeMillis(); // expire every 1min
    if (now - lastWorkerListAccess.get() > 1000 * 60) {
      synchronized (workerList) {
        workerList.clear();
      }
    }
    lastWorkerListAccess.set(now);
    synchronized (workerList) {
      return new ArrayList<WorkerInfo>(workerList);
    }
  }

  public static void invalidateWorkerInfoList() {
    synchronized (workerList) {
      workerList.clear();
    }
  }

  public static String resolve(String path) {
    if (alluxioRootPath == null) return path;
    if (path.indexOf(alluxioRootPath.toString()) == 0) return path;
    Path tpath = alluxioRootPath.resolve(path.substring(1));
    return tpath.toString();
  }

  public static URIStatus getStatus(String path) {
    path = resolve(path);
    MetaCacheData c = fcache.getIfPresent(path);
    URIStatus s = (c == null || c.getExpectMore() > System.currentTimeMillis()) ? null : c.getStatus();
    if (s != null && s.getFileId() == 0) {
      c.setStatus(null);  // volatile status work once right after ls
      LOG.debug("SMLS: unset volatile status for {}", path);
    }
    return s;
  }

  public static void setStatusExpectMore(String path) {
    path = resolve(path);
    MetaCacheData c = fcache.getIfPresent(path);
    if (c != null) {
      c.setExpectMore(System.currentTimeMillis() + 5 * 60 * 1000);
    }
  }

  private static boolean statusContain(URIStatus a, URIStatus b) {
    Set<BlockLocation> seta = new HashSet<>(), setb = new HashSet<>();
    for (FileBlockInfo f: a.getFileBlockInfos()) {
      seta.addAll(f.getBlockInfo().getLocations());
    }
    for (FileBlockInfo f: b.getFileBlockInfos()) {
      setb.addAll(f.getBlockInfo().getLocations());
    }
    return seta.containsAll(setb);
  }

  public static URIStatus setStatus(String path, URIStatus s) {
    if (!attrCacheEnabled) return null;
    if (s.getFileId() != 0 && !s.isFolder() && (s.getBlockSizeBytes() == 0 || s.getLength() == 0 
          || !s.isCompleted() || !s.isPersisted())) return null;   // always allow folder & volatile status

    path = resolve(path);
    MetaCacheData c = fcache.getIfPresent(path);
    long more = (c != null && c.getExpectMore() > System.currentTimeMillis() 
        && !statusContain(s, c.getStatus())) ? c.getExpectMore() : 0;
    c = (c == null) ? fcache.getUnchecked(path) : c;
    if (c.getStatus() != null && s.getFileId() == 0) return c.getStatus(); // not overwrite with volatile status
    c.setStatus(s);
    if (more > 0) {
      c.setExpectMore(more);
    }
    if (s.getLength() > 0) {
      for (FileBlockInfo f: s.getFileBlockInfos()) {
        BlockInfo b = f.getBlockInfo();
        setBlockInfo(b.getBlockId(), b);
      }
    }
    return s;
  }

  private static int testLocalPath = -1;
  public static boolean localBlockExisted(long id) throws Exception {
    if (testLocalPath < 0) {
      testLocalPath = Files.isDirectory(Paths.get(MetaCache.WORKER_LOCAL_PATH)) ? 1 : 0;
    } 
    return (testLocalPath == 0) ? false : Files.exists(Paths.get(MetaCache.WORKER_LOCAL_PATH + "/" + id));
  }

  public static String localBlockPath(long id) {
    return MetaCache.WORKER_LOCAL_PATH + "/" + id;
  }

  public static AlluxioURI getURI(String path) {
    path = resolve(path);
    MetaCacheData c = fcache.getUnchecked(path); //confirm to original code logic
    return (c != null) ? c.getURI() : null;
  }

  public static void invalidate(String path) {
    //also invalidate block belonging to the file
    path = resolve(path);
    MetaCacheData data = fcache.getIfPresent(path);
    if (data != null) {
      URIStatus stat = data.getStatus();
      if (stat != null) {
        for (long blockId: stat.getBlockIds()) {
          bcache.invalidate(blockId);
        }
      }
    }
    fcache.invalidate(path);
  }

  public static void invalidatePrefix(String prefix) {
    prefix = resolve(prefix);
    Set<String> keys = fcache.asMap().keySet();
    for (String k: keys) {
      if (k.startsWith(prefix)) invalidate(k);
    }
  }

  public static void invalidateAll() {
    fcache.invalidateAll();
  }

  public static void setBlockInfo(long blockId, BlockInfo info) {
    if (!blockCacheEnabled) return;

    BlockInfoData data = bcache.getUnchecked(blockId);
    if (data != null) data.setBlockInfo(info);
  }

  public static BlockInfo getBlockInfo(long blockId) {
    BlockInfoData b = bcache.getIfPresent(blockId);
    return (b != null) ? b.getBlockInfo() : null;
  }

  public static void invalidateBlockInfoCache(long blockId) {
    bcache.invalidate(blockId);
  }

  public static void invalidateAllBlockInfoCache() {
    bcache.invalidateAll();
  }

  static class MetaCacheData {
    private URIStatus uriStatus;
    private AlluxioURI uri;
    private long mExpectMore;

    public void setExpectMore(long more) {
      mExpectMore = more;
    }
    public long getExpectMore() {
      return mExpectMore;
    }

    public MetaCacheData(String path) {
      /*
         if (alluxioRootPath != null) {
         Path tpath = alluxioRootPath.resolve(path.substring(1));
         this.uri = new AlluxioURI(tpath.toString());
         } else {
         this.uri = new AlluxioURI(path);
         }*/
      this.uri = new AlluxioURI(path);
      this.uriStatus = null;
      this.mExpectMore = 0;
    }

    public URIStatus getStatus() {
      return this.uriStatus;
    }

    public void setStatus(URIStatus s) {
      this.uriStatus = s;
    }

    public AlluxioURI getURI() {
      return this.uri;
    }
  }

  static class BlockInfoData {
    Long id;
    private BlockInfo info;

    BlockInfoData(Long id) {
      this.id = id;
      this.info = null;
    }

    public void setBlockInfo(BlockInfo info) {
      this.info = info;
    }
    public BlockInfo getBlockInfo() {
      return this.info;
    }
  }

  private static class MetaCacheLoader extends CacheLoader<String, MetaCacheData> {

    /**
     * Constructs a new {@link MetaCacheLoader}.
     */
    public MetaCacheLoader() {}

    @Override
    public MetaCacheData load(String path) {
      return new MetaCacheData(path);
    }
  }

  private static class BlockInfoLoader extends CacheLoader<Long, BlockInfoData> {

    /**
     * Constructs a new {@link BlockInfoLoader}.
     */
    public BlockInfoLoader() {}

    @Override
    public BlockInfoData load(Long blockid) {
      return new BlockInfoData(blockid);
    }
  }

  static class VolatileEntry {
    public boolean mIsDirectory;
    public long mLastModifiedTimeMs;
    public String mName;

    public VolatileEntry(boolean dir, long ts, String name) {
      mIsDirectory = dir;
      mLastModifiedTimeMs = ts;
      mName = name;
    }
  }

  private static AtomicLong mWeight = new AtomicLong(0);
  private static Map<String, Set<VolatileEntry>> mDirMap = new ConcurrentHashMap<>();

  public static URIStatus file2Status(String name) {
    return file2Status(name, false, System.currentTimeMillis());
  }
  public static URIStatus file2Status(String name, boolean isDir, long ts) {
    FileInfo info = new FileInfo().setFolder(isDir).setFileId(0).setCompleted(true)
      .setName(name).setMode(Constants.DEFAULT_FILE_SYSTEM_MODE)
      .setOwner("root").setGroup("root")
      .setLastModificationTimeMs(ts).setTtlAction(alluxio.grpc.TtlAction.DELETE);
    return new URIStatus(info);
  }

  public static void addVolatileEntry(String path, boolean folder) {
    int idx = path.lastIndexOf('/');
    if (idx <= 0) return;
    String dir = path.substring(0, idx), file = path.substring(idx + 1);
    Set<VolatileEntry> value = mDirMap.get(dir);
    if (value != null) {
      evict();  // if needed
      
      value.add(new VolatileEntry(folder, System.currentTimeMillis(), file));
      mWeight.incrementAndGet();
    }
  }

  public static void removeVolatileEntry(String path) {
    int idx = path.lastIndexOf('/');
    if (idx <= 0) return;
    String dir = path.substring(0, idx), file = path.substring(idx + 1);
    Set<VolatileEntry> value = mDirMap.get(dir);
    if (value != null) {
      value.removeIf(s -> s.mName.equals(file));
      mWeight.decrementAndGet();
    }
  }

  private static void evict() {
    if (mWeight.get() > maxCachedPaths * 10) {
      for (Map.Entry<String, ?> entry: mDirMap.entrySet()) {
        if (mWeight.get() < maxCachedPaths * 7) break;
        removeVolatileList(entry.getKey());
      }
    }
  }

  public static void addVolatileList(String dir, List<URIStatus> children, boolean bigDir) {
    evict();  // if needed
    LOG.debug("SMLS: addVolatileList for {} with {} kids bigdir {}", dir, children.size(), bigDir);
    for (URIStatus s: children) {
      String key = dir.endsWith("/") ? (dir + s.getName()) : (dir  + "/" + s.getName());
      MetaCache.setStatus(key, s);
    }
    if (bigDir) return;

    Set<VolatileEntry> value = new HashSet<>();
    for (URIStatus s: children) {
      value.add(new VolatileEntry(s.isFolder(), s.getLastModificationTimeMs(), s.getName()));
    }

    if (value.size() > 0) {
      mDirMap.put(dir, value);
      mWeight.addAndGet(value.size());
    }
  }

  public static void removeVolatileList(String dir) {
    Set<VolatileEntry> value = mDirMap.get(dir);
    if (value != null) {
      LOG.debug("SMLS: removeVolatileList for {} with size {}", dir, value.size());
      mWeight.addAndGet(-value.size());
      mDirMap.remove(dir);
    }
  }

  public static boolean hasVolatileList(String dir) {
    return mDirMap.containsKey(dir);
  }

  /**
   * for ls with big dir, don't do getStatus for each file, return incomplete info
   */
  public static List<URIStatus> getVolatileList(String dir) {
    Set<VolatileEntry> v = mDirMap.get(dir);
    List<URIStatus> ret = new ArrayList<>();
    if (v != null) {
      for (VolatileEntry e: v) {
        String key = dir.endsWith("/") ? (dir + e.mName) : (dir  + "/" + e.mName);
        URIStatus s = file2Status(e.mName, e.mIsDirectory, e.mLastModifiedTimeMs);
        URIStatus us = MetaCache.setStatus(key, s);
        ret.add((us == null) ? s : us);
      }
      LOG.debug("SMLS: getVolatileList for {} with size {}", dir, ret.size());
    }
    return (v == null) ? null : ret;
  }
}

