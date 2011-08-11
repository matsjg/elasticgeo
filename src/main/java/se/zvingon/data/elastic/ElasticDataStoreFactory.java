package se.zvingon.data.elastic;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFactorySpi;

import java.awt.RenderingHints.Key;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

public class ElasticDataStoreFactory implements DataStoreFactorySpi {

    public static final Param HOSTNAME = new Param("SearchHost", String.class, "Hostname", true);
    public static final Param HOSTPORT = new Param("SearchPort", Integer.class, "Port", true);
    public static final Param INDEX_NAME = new Param("Indexname", String.class, "name of index", true);
    public static final Param ESQUERY = new Param("Elasticsearch query", String.class, "Query to elastic search", false);

    public String getDisplayName() {
        return "ElasticSearch";
    }

    public String getDescription() {
        return "ElasticSearch";
    }

    public Param[] getParametersInfo() {
        return new Param[]{HOSTNAME, HOSTPORT, INDEX_NAME, ESQUERY};
    }

    public boolean canProcess(Map<String, Serializable> params) {
        try {
            String searchHost = (String) HOSTNAME.lookUp(params);
            String indexName = (String) INDEX_NAME.lookUp(params);
            Integer hostport = (Integer) HOSTPORT.lookUp(params);
            String query = (String) ESQUERY.lookUp(params);
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
        Integer hostPort = (Integer) HOSTPORT.lookUp(params);
        String query = (String) ESQUERY.lookUp(params);
        return new ElasticDataStore(searchHost, hostPort, indexName, query);
    }

    public DataStore createNewDataStore(Map<String, Serializable> params) throws IOException {
        System.out.println("createNewDataStore");
        return null;
    }

}
