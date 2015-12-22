package lepka.zookeeper.fuse;

import jnr.ffi.Pointer;
import jnr.ffi.types.mode_t;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;

import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class ZkFuse extends FuseStubFS {

    private final CuratorFramework curator;
    private final TreeCache cache;

    private static final Logger LOGGER = LoggerFactory.getLogger(ZkFuse.class);

    private ZkFuse(String connectionString,String zPath) {
        curator = CuratorFrameworkFactory.newClient(connectionString, new ExponentialBackoffRetry(1000, 5));
        cache = new TreeCache(curator, zPath);
    }

    @Override
    public final Pointer init(Pointer conn) {
        curator.start();
        CountDownLatch latch = new CountDownLatch(1);
        cache.getListenable().addListener((curatorFramework, treeCacheEvent) -> {
            if (treeCacheEvent.getType() == TreeCacheEvent.Type.INITIALIZED) {
                latch.countDown();
            }
        });
        try {
            cache.start();
        } catch (Exception e) {
            LOGGER.error("Could not initialize tree cache...");
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            LOGGER.error("Something went too wrong...");
        }
        LOGGER.info("ZkFuse initialized...");
        return conn;
    }

    @Override
    public final void destroy(Pointer initResult) {
        curator.close();
        cache.close();
    }

    @Override
    public final int readdir(String path, Pointer buf, FuseFillDir filter, @off_t long offset, FuseFileInfo fi) {
        Map<String, ChildData> children = cache.getCurrentChildren(path);
        if (children == null) {
            return -ErrorCodes.ENOENT();
        }
        children.forEach((key, value) -> filter.apply(buf, key, null, 0L));
        return 0;
    }

    @Override
    public final int getattr(String path, FileStat stat) {
        ChildData data = cache.getCurrentData(path);
        if (data == null) {
            return -ErrorCodes.ENOENT();
        }
        Stat zkStat = data.getStat();
        if (zkStat.getNumChildren() > 0) {
            stat.st_mode.set(FileStat.S_IFDIR | 0755);
        } else {
            stat.st_mode.set(FileStat.S_IFREG | 0777);
            stat.st_size.set(zkStat.getDataLength());
        }
        return 0;
    }

    @Override
    public final int read(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
        ChildData data = cache.getCurrentData(path);
        if (data == null) {
            return -ErrorCodes.ENOENT();
        }
        byte[] bytes = data.getData();
        buf.put(0, bytes, 0, bytes.length);
        return data.getStat().getDataLength();
    }

    @Override
    public final int mkdir(String path, @mode_t long mode) {
        int errorCode = createNode(path);
        if (errorCode != 0) {
            return errorCode;
        }

        //hack for creating folders
        return createNode(path + "/.empty");
    }

    @Override
    public final int create(String path, @mode_t long mode, FuseFileInfo fi) {
        return createNode(path);
    }

    private int createNode(String path) {
        CountDownLatch latch = new CountDownLatch(1);
        cache.getListenable().addListener((client, event) -> {
            if (event.getType() == TreeCacheEvent.Type.NODE_ADDED &&
                    client.checkExists().forPath(path) != null) {
                latch.countDown();
            }
        });
        if (cache.getCurrentData(path) != null) {
            return -ErrorCodes.EEXIST();
        }
        try {
            curator.create().forPath(path, new byte[]{});
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            return -ErrorCodes.EREMOTEIO();
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            LOGGER.error("Something went too wrong...");
        }
        return 0;
    }

    @Override
    public final int write(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
        try {
            byte[] bytes = new byte[(int) size];
            buf.get(0, bytes, 0, (int) size);
            curator.setData().forPath(path, bytes);
            return (int) size;
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            return -ErrorCodes.EREMOTEIO();
        }
    }

    @Override
    public final int truncate(String path, @off_t long size) {
        return 0;
    }

    //TODO rename

    @Override
    public final int rmdir(String path) {
        return deleteNode(path);
    }

    @Override
    public final int unlink(String path) {
        return deleteNode(path);
    }

    private int deleteNode(String path) {
        try {
            curator.delete().deletingChildrenIfNeeded().forPath(path);
            return 0;
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            return ErrorCodes.EREMOTEIO();
        }
    }

    public static void main(String... args) {
        if (args.length < 3) {
            LOGGER.error("usage: java -jar ZkFuse.jar [connectString] [znodePath] [mountPoint]");
            System.exit(1);
        }

        ZkFuse stub = new ZkFuse(args[0],args[1]);

        try {
            stub.mount(Paths.get(args[2]), true);
        } finally {
            stub.umount();
        }
    }
}
