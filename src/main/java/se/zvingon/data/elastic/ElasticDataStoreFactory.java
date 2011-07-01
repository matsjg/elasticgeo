package se.zvingon.data.elastic;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFactorySpi;

import java.awt.RenderingHints.Key;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

public class ElasticDataStoreFactory implements DataStoreFactorySpi {

    public static final Param SEARCH_HOST = new Param("SearchUri", String.class, "Uri to elastic search host and index", true);
    public static final Param INDEX_NAME = new Param("Indexname", String.class, "name of index", true);
    public static final Param ESQUERY = new Param("Elasticsearch query", String.class, "Query to elastic search", false);

    public String getDisplayName() {
        return "ElasticSearch";
    }

    public String getDescription() {
        return "ElasticSearch";
    }

    public Param[] getParametersInfo() {
        return new Param[]{SEARCH_HOST, INDEX_NAME, ESQUERY};
    }

    public boolean canProcess(Map<String, Serializable> params) {
        try {
            String searchHost = (String) SEARCH_HOST.lookUp(params);
            String indexName = (String) INDEX_NAME.lookUp(params);
            String query = (String) ESQUERY.lookUp(params);
            if (searchHost != null && indexName != null) {
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
        String searchHost = (String) SEARCH_HOST.lookUp(params);
        String indexName = (String) INDEX_NAME.lookUp(params);
        String query = (String) ESQUERY.lookUp(params);
        return new ElasticDataStore(searchHost, indexName, query);
    }

    public DataStore createNewDataStore(Map<String, Serializable> params) throws IOException {
        System.out.println("createNewDataStore");
        return null;
    }

}
