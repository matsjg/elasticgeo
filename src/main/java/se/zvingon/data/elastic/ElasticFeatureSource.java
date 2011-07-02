package se.zvingon.data.elastic;

import com.vividsolutions.jts.geom.Point;
import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureCollection;
import org.geotools.data.store.ContentFeatureStore;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.restlet.Client;
import org.restlet.data.Protocol;
import org.restlet.ext.json.JsonRepresentation;

import java.io.IOException;
import java.util.Iterator;

@SuppressWarnings("unchecked")
public class ElasticFeatureSource extends ContentFeatureStore {

    public ElasticFeatureSource(ContentEntry entry, Query query) {
        super(entry, query);
    }

    /**
     * Access parent datastore
     */
    public ElasticDataStore getDataStore() {
        return (ElasticDataStore) super.getDataStore();
    }

    /**
     * Implementation that generates the total bounds
     */
    protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
        ReferencedEnvelope bounds = new ReferencedEnvelope(getSchema().getCoordinateReferenceSystem());

        FeatureReader<SimpleFeatureType, SimpleFeature> featureReader = getReaderInternal(query);
        try {
            while (featureReader.hasNext()) {
                SimpleFeature feature = featureReader.next();
                bounds.include(feature.getBounds());
            }
        } finally {
            featureReader.close();
        }
        return bounds;
    }

    protected int getCountInternal(Query query) throws IOException {
        ContentFeatureCollection cfc = this.getFeatures(query);
        int count = 0;
        SimpleFeatureIterator iter = cfc.features();
        while (iter.hasNext()) {
            iter.next();
            count++;
        }
        iter.close();
        return count;
    }

    protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query)
            throws IOException {
        return new ElasticFeatureReader(getState(), query);
    }

    protected SimpleFeatureType buildFeatureType() throws IOException {

        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
        builder.setName(entry.getName());

        ElasticDataStore dataStore = getDataStore();

        Client client = new Client(Protocol.HTTP);
        String mappingUri = dataStore.searchHost + "/" + dataStore.indexName + "/" + entry.getName().getLocalPart() + "/_mapping";
        System.out.println("Calling: " + mappingUri);
        org.restlet.data.Response response = client.get(mappingUri);
        JsonRepresentation representation = new JsonRepresentation(response.getEntity());
        JSONObject parent = null;
        try {
            JSONObject query = new JSONObject(dataStore.query);
            JSONArray fields = new JSONArray();
            if (query.has("fields")) {
                fields = query.getJSONArray("fields");
            }

            parent = representation.toJsonObject();
            JSONObject type = parent.getJSONObject(entry.getName().getLocalPart());
            JSONObject properties = type.getJSONObject("properties");
            Iterator propertyNameIter = properties.keys();
            while (propertyNameIter.hasNext()) {
                String propertyKey = (String) propertyNameIter.next();

                boolean shouldInclude = false;
                for (int i = 0; i < fields.length(); i++) {
                    if (propertyKey.equals(fields.get(i))) {
                        shouldInclude = true;
                        break;
                    }
                }
                if (!shouldInclude) {
                    continue;
                }
                JSONObject property = properties.getJSONObject(propertyKey);
                if (property.has("type")) {
                    String propertyType = property.getString("type");
                    if ("geo_point".equalsIgnoreCase(propertyType)) {
                        builder.setCRS(DefaultGeographicCRS.WGS84);
                        builder.add(propertyKey, Point.class);
                        builder.setSRS("EPSG:4326");
                    }
                    if ("string".equalsIgnoreCase(propertyType)) {
                        builder.add(propertyKey, String.class);
                    }
                }
            }

        } catch (JSONException e) {
            throw new IOException(e);
        }
        final SimpleFeatureType SCHEMA = builder.buildFeatureType();
        return SCHEMA;
    }

    @Override
    protected FeatureWriter<SimpleFeatureType, SimpleFeature> getWriterInternal(Query query, int flags) throws IOException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

}
