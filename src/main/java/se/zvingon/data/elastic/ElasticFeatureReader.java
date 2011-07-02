package se.zvingon.data.elastic;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.store.ContentState;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeType;
import org.restlet.Client;
import org.restlet.data.Protocol;
import org.restlet.data.Response;
import org.restlet.ext.json.JsonRepresentation;

import java.io.IOException;
import java.util.NoSuchElementException;

public class ElasticFeatureReader implements FeatureReader<SimpleFeatureType, SimpleFeature> {

    protected ContentState state;
    private SimpleFeature next;
    protected SimpleFeatureBuilder builder;
    private int row;
    private GeometryFactory geometryFactory;
    JSONArray data;

    public ElasticFeatureReader(ContentState contentState, Query query) throws IOException {
        this.state = contentState;
        builder = new SimpleFeatureBuilder(state.getFeatureType());
        geometryFactory = JTSFactoryFinder.getGeometryFactory(null);
        row = 0;


        // todo fix

        ElasticDataStore dataStore = (ElasticDataStore) contentState.getEntry().getDataStore();


        String mappingUri = dataStore.searchHost + "/" + dataStore.indexName + "/" + state.getEntry().getName().getLocalPart() + "/_search?";
        System.out.println("Calling: " + mappingUri);
        Client client = new Client(Protocol.HTTP);
        try {
            Response response = client.post(mappingUri, new JsonRepresentation(new JSONObject(dataStore.query)));
            JsonRepresentation representation = new JsonRepresentation(response.getEntity());
            data = representation.toJsonObject().getJSONObject("hits").getJSONArray("hits");
        } catch (JSONException e) {
            throw new IOException(e);
        }


    }

    public SimpleFeatureType getFeatureType() {
        return state.getFeatureType();
    }

    public SimpleFeature next() throws IOException, IllegalArgumentException,
            NoSuchElementException {
        SimpleFeature feature;
        if (next != null) {
            feature = next;
            next = null;
        } else {
            feature = readFeature();
        }
        return feature;
    }

    SimpleFeature readFeature() throws IOException {
        try {
            if (row > data.length() - 1) {
                return null;
            }
            JSONObject hit = (JSONObject) data.get(row);

            JSONObject feature;
            if (hit.has("_source")) {
                feature = (JSONObject) hit.get("_source");
            } else if (hit.has("fields")) {
                feature = (JSONObject) hit.get("fields");
            } else {
                throw new IOException("No result found!");
            }

            SimpleFeatureType type = getFeatureType();
            for (AttributeType attributeType : type.getTypes()) {
                String propertyKey = attributeType.getName().getLocalPart();
                if (feature.has(propertyKey)) {

                    if (Point.class.equals(attributeType.getBinding())) {
                        Coordinate coordinate = new Coordinate();
                        JSONObject point = (JSONObject) feature.getJSONObject(propertyKey);
                        coordinate.y = point.getDouble("lat");
                        coordinate.x = point.getDouble("lon");
                        builder.set(propertyKey, geometryFactory.createPoint(coordinate));
                    } else {
                        builder.set(propertyKey, feature.get(propertyKey));
                    }
                }
            }
        } catch (JSONException e) {
            // todo fix this
            e.printStackTrace();
        }
        return this.buildFeature();
    }

    protected SimpleFeature buildFeature() {
        row += 1;
        return builder.buildFeature(state.getEntry().getTypeName() + "." + row);
    }

    public boolean hasNext() throws IOException {
        if (next != null) {
            return true;
        } else {
            next = readFeature();
            return next != null;
        }
    }

    public void close() throws IOException {
        builder = null;
        geometryFactory = null;
        next = null;
    }

}
