package scats.utils.geotools.factory;

import com.google.common.base.Joiner;
import com.vividsolutions.jts.geom.Point;
import org.geotools.factory.Hints;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.geomesa.accumulo.index.Constants;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.locationtech.geomesa.utils.interop.WKTUtils;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.FactoryException;
import scala.Tuple2;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Yikai Gong
 */

public class ScatsDOWFeatureFactory {

    public static final String FT_NAME = "ScatsDayOfWeek";

    public static final String NB_SCATS_SITE = "NB_SCATS_SITE";
    public static final String NB_DETECTOR = "NB_DETECTOR";
    public static final String DAY_OF_WEEK = "DAY_OF_WEEK";
    public static final String TIME_OF_DAY = "TIME_OF_DAY";
    public static final String AVERAGE_VEHICLE_COUNT = "AVERAGE_VEHICLE_COUNT";
    public static final String NUM_OF_FEATURES = "NUM_OF_FEATURES";

    public static SimpleFeatureType createFeatureType() throws SchemaException, FactoryException {
        List<String> attributes = new ArrayList<>();
        attributes.add("*geometry:Point:srid=4326");            //indexed
        attributes.add(NB_SCATS_SITE + ":String:index=join");   //indexed
        attributes.add(NB_DETECTOR + ":String");
        attributes.add(DAY_OF_WEEK + ":String:index=join");     //indexed
        attributes.add(TIME_OF_DAY + ":Integer:index=join");    //indexed
        attributes.add(AVERAGE_VEHICLE_COUNT + ":Double");
        attributes.add(NUM_OF_FEATURES + ":Integer");

        // create the bare simple-feature type
        String simpleFeatureTypeSchema = Joiner.on(",").join(attributes);
        SimpleFeatureType simpleFeatureType =
                SimpleFeatureTypes.createType(FT_NAME, simpleFeatureTypeSchema);
        // use the user-data (hints) to specify which date-time field is meant to be indexed;
        simpleFeatureType.getUserData().put(Constants.SF_PROPERTY_START_TIME, TIME_OF_DAY);
        return simpleFeatureType;
    }

    public static SimpleFeatureBuilder getFeatureBuilder() throws SchemaException, FactoryException {
        SimpleFeatureType simpleFeatureType = createFeatureType();
        return new SimpleFeatureBuilder(simpleFeatureType);
    }

    public static List<SimpleFeature> buildFeaturesFromTuple(Tuple2<String, double[]> tuple, SimpleFeatureBuilder builder) throws ParseException {
        List<SimpleFeature> result = new ArrayList<>();
        System.out.println(tuple._1);
        String[] keys = tuple._1.split("#");
        String scatsSite = keys[0];
        String detectorNum = keys[1];
        String dayOfWeek = keys[2];
        String geo_wkt = keys[3];
        System.out.println(Arrays.asList(keys));
        System.out.println(geo_wkt);
        Point point = (Point) WKTUtils.read(geo_wkt);
        System.out.println(point);
        point.setSRID(4326);

        for (int i = 0; i < 96; i++) {
            Integer timeOfDay = i * 15;
            String fid = scatsSite + "-" + detectorNum + "-" + dayOfWeek + "-" + timeOfDay;
            builder.reset();
            SimpleFeature simpleFeature = builder.buildFeature(fid);
            // Tell GeoMesa to use user provided FID
            simpleFeature.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);
            simpleFeature.setAttribute("geometry", point);
            simpleFeature.setAttribute(NB_SCATS_SITE, scatsSite);
            simpleFeature.setAttribute(NB_DETECTOR, detectorNum);
            simpleFeature.setAttribute(DAY_OF_WEEK, dayOfWeek);
            simpleFeature.setAttribute(TIME_OF_DAY, timeOfDay);
            simpleFeature.setAttribute(AVERAGE_VEHICLE_COUNT, tuple._2[i]);
            simpleFeature.setAttribute(NUM_OF_FEATURES, tuple._2[96]);
            result.add(simpleFeature);
        }
        return result;
    }

    public static List<SimpleFeature> buildFeaturesFromTuple(Tuple2<String, double[]> tuple) throws ParseException, SchemaException, FactoryException {
        SimpleFeatureBuilder builder = getFeatureBuilder();
        return buildFeaturesFromTuple(tuple, builder);
    }
}
