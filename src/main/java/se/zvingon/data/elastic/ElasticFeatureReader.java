package se.zvingon.data.elastic;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.geotools.data.DataStore;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.store.ContentState;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.visitor.ExtractBoundsFilterVisitor;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterVisitor;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Vector;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.index.query.FilterBuilders.geoBoundingBoxFilter;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.node.NodeBuilder.*;


public class ElasticFeatureReader implements FeatureReader<SimpleFeatureType, SimpleFeature> {

    protected ContentState state;
    private SimpleFeature next;
    protected SimpleFeatureBuilder builder;
    private int row;
    private GeometryFactory geometryFactory;
    SearchHit[] data;
    ElasticDataStore dataStore;
    long count = 0;
    Client client;
    SearchResponse response;
    Iterator<SearchHit> searchHitIterator;

    Vector<ListenableActionFuture<SearchResponse>> responses = new Vector<ListenableActionFuture<SearchResponse>>();
    private long nbrOfResponses;
    private int internalIndex;
    private Client searchClient;


    public ElasticFeatureReader(ContentState contentState, Query query) throws IOException {


        this.state = contentState;
        builder = new SimpleFeatureBuilder(state.getFeatureType());
        geometryFactory = JTSFactoryFinder.getGeometryFactory(null);
        row = 1;
        internalIndex = 1;
        dataStore = (ElasticDataStore) contentState.getEntry().getDataStore();
        searchClient = dataStore.elasticSearchNode.client();


        Filter filter = query.getFilter();
        FilterVisitor visitor = ExtractBoundsFilterVisitor.BOUNDS_VISITOR;
        Envelope result = (Envelope) filter.accept(visitor, DefaultGeographicCRS.WGS84);
        System.out.println("Read based on envelope: " + result.toString());

        client = new TransportClient().addTransportAddress(new InetSocketTransportAddress(dataStore.searchHost, dataStore.hostPort));

        FilterBuilder geoFilter = geoBoundingBoxFilter("location")
                .topLeft(result.getMaxY(), result.getMinX())
                .bottomRight(result.getMinY(), result.getMaxX())
                .cache(true);
        try {
            SearchResponse countRequest = client.prepareSearch(dataStore.indexName)
                    .setTypes(dataStore.getTypeNames())
                    .addFields("objekt_id", "objekttyp_id", "location")
                    .setSearchType(SearchType.COUNT)
                    .setQuery(matchAllQuery())
                    .setFilter(geoFilter)
                    .execute().get();

            count = countRequest.getHits().getTotalHits();

            System.out.println("Found " + count + " features matching bbox");

            response = searchClient.prepareSearch(dataStore.indexName)
                    .setTypes(dataStore.getTypeNames())
                    .setQuery(matchAllQuery())
                    .setFilter(geoFilter)
                    .setFrom(0)
                    .setSize(new Long(count).intValue())
                    .addFields("objekt_id", "objekttyp_id", "location")
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .execute().get();

            //new Long(count).intValue()
            System.out.println(response.hits().getTotalHits());
            searchHitIterator = response.getHits().iterator();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
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
        if (row > count) {
            return null;
        }

        if (!searchHitIterator.hasNext()) {
            return null;
        }


        SearchHit hit = searchHitIterator.next();
        SimpleFeatureType type = getFeatureType();
        for (AttributeType attributeType : type.getTypes()) {
            String propertyKey = attributeType.getName().getLocalPart();
            if (hit.getFields().containsKey(propertyKey)) {
                SearchHitField field = hit.getFields().get(propertyKey);
                if (Point.class.equals(attributeType.getBinding())) {
                    Coordinate coordinate = new Coordinate();
                    Map<String, Object> location = (Map<String, Object>) field.getValue();
                    // this fix this

                    if (location.containsKey("lat") && location.containsKey("lon")) {
                        coordinate.y = (Double) location.get("lat");
                        coordinate.x = (Double) location.get("lon");
                        builder.set(propertyKey, geometryFactory.createPoint(coordinate));
                    }
                } else {
                    builder.set(propertyKey, field.getValue().toString());
                }
            }
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
        client.close();
        client = null;
        searchClient.close();
        searchClient = null;
        searchHitIterator = null;
        response = null;
    }

}
