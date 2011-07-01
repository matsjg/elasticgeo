// header start
package se.zvingon.data.elastic;

import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.NameImpl;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;
import org.restlet.Client;
import org.restlet.data.Protocol;
import org.restlet.data.Response;
import org.restlet.ext.json.JsonRepresentation;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

public class ElasticDataStore extends ContentDataStore {
// header end

    // constructor start
    String searchHost;
    String indexName;
    String query;

    public ElasticDataStore(String searchHost, String indexName, String query) {
        this.searchHost = searchHost;
        this.indexName = indexName;
        this.query = query;
    }

    // createTypeNames start
    @Override
    protected List<Name> createTypeNames() throws IOException {
        // todo hämta index från elastic search
        Client client = new Client(Protocol.HTTP);
        Response response = client.get(searchHost + "/_mapping");
        JsonRepresentation representation = new JsonRepresentation(response.getEntity());
        JSONObject parent = null;
        List<Name> types = new Vector<Name>();
        try {
            parent = representation.toJsonObject();
            JSONObject index = parent.getJSONObject(indexName);
            Iterator typeIter = index.keys();
            while (typeIter.hasNext()) {
                types.add(new NameImpl((String) typeIter.next()));
            }
        } catch (JSONException e) {
            throw new IOException(e);
        }
        return types;
    }
    // createTypeNames end


    @Override
    protected ContentFeatureSource createFeatureSource(ContentEntry entry) throws IOException {
        return new ElasticFeatureSource(entry, Query.ALL);
    }

    @Override
    public FeatureReader<SimpleFeatureType, SimpleFeature> getFeatureReader(Query query, Transaction tx) throws IOException {
        // todo use query?
        return super.getFeatureReader(query, tx);    //To change body of overridden methods use File | Settings | File Templates.
    }

}
