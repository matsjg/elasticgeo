package se.zvingon.data.elastic;

import org.elasticsearch.cluster.ClusterName;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFactorySpi;

import java.awt.RenderingHints.Key;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Vector;

public class ElasticDataStoreFactory implements DataStoreFactorySpi {

    public static final Param HOSTNAME = new Param("SearchHost", String.class, "Hostname", true);
    public static final Param HOSTPORT = new Param("SearchPort", Integer.class, "Port", true);
    public static final Param INDEX_NAME = new Param("Indexname", String.class, "name of index", true);
    public static final Param LOCAL_NODE = new Param("Use Elasticsearch local node", Boolean.class, "Use local node", false);
    public static final Param CLUSTERNAME = new Param("Cluster name", String.class, "Attach local node to cluster with name", false);
    public static final Param STORE_DATA = new Param("Store data in local node", Boolean.class, "Store data in local node", false);
    public static final Param FIELDS = new Param("Fields", String.class, "Fields", false);

    public String getDisplayName() {
        return "ElasticSearch";
    }

    public String getDescription() {
        return "ElasticSearch";
    }

    public Param[] getParametersInfo() {
        return new Param[]{HOSTNAME, HOSTPORT, INDEX_NAME, CLUSTERNAME, LOCAL_NODE, STORE_DATA, FIELDS};
    }

    public boolean canProcess(Map<String, Serializable> params) {
        try {
            String searchHost = (String) HOSTNAME.lookUp(params);
            String indexName = (String) INDEX_NAME.lookUp(params);
            Integer hostport = (Integer) HOSTPORT.lookUp(params);
            if (searchHost != null && hostport != null && indexName != null) {
                return true;
            }
        } catch (IOException e) {
            // ignore
        }
        return false;
    }

    public boolean isAvailable() {
        return true;
    }

    public Map<Key, ?> getImplementationHints() {
        return null;
    }

    public DataStore createDataStore(Map<String, Serializable> params) throws IOException {
        System.out.println("createDataStore");
        String searchHost = (String) HOSTNAME.lookUp(params);
        String indexName = (String) INDEX_NAME.lookUp(params);
        String clusterName = (String) CLUSTERNAME.lookUp(params);
        Integer hostPort = (Integer) HOSTPORT.lookUp(params);
        Boolean storeData = (Boolean) STORE_DATA.lookUp(params);
        if (storeData == null)
            storeData = false;
        Boolean localNode = (Boolean) LOCAL_NODE.lookUp(params);
        if (localNode == null)
            localNode = false;

        String fields = (String) FIELDS.lookUp(params);
        List<String> fieldList = null;
        if (fields != null) {
            fieldList = Arrays.asList(fields.split(","));
        }

        return new ElasticDataStore(searchHost, hostPort, indexName, clusterName, localNode, storeData, fieldList);
    }

    public DataStore createNewDataStore(Map<String, Serializable> params) throws IOException {
        System.out.println("createNewDataStore");
        return null;
    }

}
