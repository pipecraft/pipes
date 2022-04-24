package org.pipecraft.infra.monitoring;

import java.io.File;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

/**
 * A monitorable covering different JVM and runtime aspects
 * 
 * TODO(Eyal): Consider implementing a generic JMXBean-to-JSON converter, so that any MXBean can be exposed
 * as a JsonMonitorable easily.
 * 
 * @author Eyal Schneider
 */
public class SystemHealthMonitorable implements JsonMonitorable {
  
    private final MemoryMXBean memBean;
    private final OperatingSystemMXBean osBean;
    private final ThreadMXBean threadMXBean;
    private final RuntimeMXBean runtimeMXBean;

    public SystemHealthMonitorable() {
      memBean = ManagementFactory.getMemoryMXBean();      
      osBean = ManagementFactory.getOperatingSystemMXBean();      
      threadMXBean = ManagementFactory.getThreadMXBean();
      runtimeMXBean = ManagementFactory.getRuntimeMXBean();
    }
  
    @Override
    public JSONObject getOwnMetrics() {
      List<MemoryPoolMXBean> memPools = ManagementFactory.getMemoryPoolMXBeans();
      List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
      
      JSONObject res = new JSONObject();      
      res.put("heap", getMemUsage(memBean.getHeapMemoryUsage()));
      res.put("disk", getDiskUsage());
      res.put("nonHeap", getMemUsage(memBean.getNonHeapMemoryUsage()));
      res.put("gc", getGCData(gcBeans));
      res.put("processorsCount", osBean.getAvailableProcessors());
      res.put("loadAvg", osBean.getSystemLoadAverage());
      res.put("upTime", runtimeMXBean.getUptime());
      res.put("threadsCount", threadMXBean.getThreadCount());
      res.put("threadsStarted", threadMXBean.getTotalStartedThreadCount());
      res.put("tomcat", getTomcatData());
      res.put("memAfterGC", getMemPoolsData(memPools));

      return res;
    }

    private JSONObject getMemPoolsData(List<MemoryPoolMXBean> memPools) {
      JSONObject res = new JSONObject();
      for (MemoryPoolMXBean pool : memPools) {
        MemoryUsage collectionUsage = pool.getCollectionUsage();
        JSONObject details = null;
        if (collectionUsage != null)
          details = getMemUsage(collectionUsage);
        res.put(pool.getName(), details);
      }
      return res;
    }

    private static JSONArray getGCData(List<GarbageCollectorMXBean> gcBeans) {
      JSONArray arr = new JSONArray();
      for (GarbageCollectorMXBean gcBean : gcBeans) {
        JSONObject gc = new JSONObject();
        gc.put("name", gcBean.getName());
        gc.put("collectionsCount", gcBean.getCollectionCount());
        gc.put("collectionTime", gcBean.getCollectionTime());
        gc.put("memPools", Arrays.toString(gcBean.getMemoryPoolNames()));
        arr.add(gc);
      }
      return arr;
    }

    private static JSONObject getMemUsage(MemoryUsage memUsage) {
      JSONObject res = new JSONObject();
      res.put("init", memUsage.getInit());
      res.put("committed", memUsage.getCommitted());
      res.put("used", memUsage.getUsed());
      res.put("max", memUsage.getMax());
      return res;
    }

    private static JSONArray getDiskUsage() {
      JSONArray res = new JSONArray();
      for(File root : File.listRoots()) {
        JSONObject rootJson = new JSONObject();
        rootJson.put("name", root.getAbsolutePath());
        rootJson.put("usagePerc", 100.0 - root.getFreeSpace() * 100.0 / root.getTotalSpace());
        res.add(rootJson);
      }
      return res;
    }

    private static JSONObject getTomcatData() {
      JSONObject res = new JSONObject();
      try {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        
        ObjectName beanPattern = new ObjectName("Catalina:type=ThreadPool,name=*");
        Set<ObjectInstance> beans = mbs.queryMBeans(beanPattern, null);
        for (ObjectInstance inst : beans) {
          ObjectName beanName = inst.getObjectName();
          JSONObject poolJs = new JSONObject();
          String name = (String) mbs.getAttribute(beanName, "name");
          int currThreadsCount = (Integer) mbs.getAttribute(beanName, "currentThreadCount");
          int currThreadsBusy = (Integer) mbs.getAttribute(beanName, "currentThreadsBusy");
          int maxThreadsCount = (Integer) mbs.getAttribute(beanName, "maxThreads");
          long connectionsCount = (Long) mbs.getAttribute(beanName, "connectionCount");
          int maxConnections = (Integer) mbs.getAttribute(beanName, "maxConnections");
          int maxKeepAliveReq = (Integer) mbs.getAttribute(beanName, "maxKeepAliveRequests");
          int keepAliveTO = (Integer) mbs.getAttribute(beanName, "keepAliveTimeout");
          poolJs.put("currentThreadCount", currThreadsCount);
          poolJs.put("currentThreadBusy", currThreadsBusy);
          poolJs.put("maxThreads", maxThreadsCount);
          poolJs.put("connections", connectionsCount);
          poolJs.put("maxConnections", maxConnections);
          poolJs.put("maxKeepAliveRequests", maxKeepAliveReq);
          poolJs.put("keepAliveTimeout", keepAliveTO);
          res.put(name, poolJs);
        }
      } catch (Exception e) {
        //Silently ignore
      }
      return res;
    }

    @Override
    public Map<String, ? extends JsonMonitorable> getChildren() {
      return Collections.emptyMap();
    }
}
