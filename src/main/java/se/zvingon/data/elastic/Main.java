package se.zvingon.data.elastic;

import org.geotools.feature.NameImpl;
import org.json.JSONException;
import org.json.JSONObject;
import org.opengis.feature.type.Name;
import org.restlet.Client;
import org.restlet.data.Protocol;
import org.restlet.data.Response;
import org.restlet.ext.json.JsonRepresentation;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

/**
 * Created by IntelliJ IDEA.
 * User: mats
 * Date: 2011-07-01 v.26
 * Time: 21.42
 * To change this template use File | Settings | File Templates.
 */
public class Main {


    public static void main(String[] args) throws IOException {

        Client client = new Client(Protocol.HTTP);
        Response response = client.get("http://localhost:9200/hemnet/_mapping");
        JsonRepresentation representation = new JsonRepresentation(response.getEntity());
        JSONObject parent = null;
        List<Name> types = new Vector<Name>();
        try {
            parent = representation.toJsonObject();
            JSONObject index = parent.getJSONObject("hemnet");
            Iterator typeIter = index.keys();
            while (typeIter.hasNext()) {
                String name = (String) typeIter.next();
                System.out.println(name);
                types.add(new NameImpl(name));

            }
        } catch (JSONException e) {
            throw new IOException(e);
        }




    }

}
