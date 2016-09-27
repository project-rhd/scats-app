package scats.utils.geotools.factory;

import com.google.common.base.Joiner;
import com.vividsolutions.jts.geom.*;
import org.apache.spark.sql.Row;
import org.geotools.factory.Hints;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.CRS;
import org.locationtech.geomesa.accumulo.index.Constants;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.locationtech.geomesa.utils.interop.WKTUtils;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author Yikai Gong
 */

public class ScatsFeaturePointFactory {

    public static final String FT_NAME = "ScatsVolumeData";

    public static SimpleFeatureType createFeatureType() throws SchemaException, FactoryException {
        List<String> attributes = new ArrayList<>();
        attributes.add("*geometry:Point:srid=4326");
        attributes.add("NB_SCATS_SITE:String:index=join");
        attributes.add("QT_INTERVAL_COUNT:Date");
        attributes.add("NB_DETECTOR:String");
        attributes.add("DAY_OF_WEEK:String:index=join");
        for (int i = 0; i < 96; i++) {
            String attribute;
            if (i < 10)
                attribute = "V0" + Integer.toString(i) + ":String";
            else
                attribute = "V" + Integer.toString(i) + ":String";
            attributes.add(attribute);
        }
        attributes.add("DS_LOCATION:String");
        attributes.add("NB_LANE:String");
        attributes.add("LANE_MVT:String");
        attributes.add("LOC_MVT:String");
        attributes.add("HF:String:index=join");
        attributes.add("unique_road:MultiLineString:srid=4326");

        // create the bare simple-feature type
        String simpleFeatureTypeSchema = Joiner.on(",").join(attributes);
        SimpleFeatureType simpleFeatureType =
                SimpleFeatureTypes.createType("ScatsVolumeData", simpleFeatureTypeSchema);
        // use the user-data (hints) to specify which date-time field is meant to be indexed;
        simpleFeatureType.getUserData().put(Constants.SF_PROPERTY_START_TIME, "QT_INTERVAL_COUNT");
        return simpleFeatureType;
    }

    public static SimpleFeatureBuilder getFeatureBuilder() throws SchemaException, FactoryException {
        SimpleFeatureType simpleFeatureType = createFeatureType();
        return new SimpleFeatureBuilder(simpleFeatureType);
    }

    public static SimpleFeature buildFeatureFromRow(Row row, SimpleFeatureBuilder builder) throws ParseException {
        String wktPoint = row.getString(0);
        String wktLine = row.getString(105);
        Point point = null;
        MultiLineString line = null;
        if (wktPoint!= null && !wktPoint.equals("null")){
            point = (Point) WKTUtils.read(wktPoint);
            point.setSRID(4326);
        } else{
            // Since GeoMesa do not allow null for default geometry,
            // Skip this row by return null.
            return null;
        }
        if (wktLine != null && !wktLine.equals("null")){
            Geometry geo = WKTUtils.read(wktLine);
            if (geo instanceof MultiLineString)
                line = (MultiLineString)geo;
            else if (geo instanceof LineString){
                LineString[] lineStrings = new LineString[]{(LineString) geo};
                line = new GeometryFactory().createMultiLineString(lineStrings);
            }
            if (line != null)
                line.setSRID(4326);
        }

        String scatsSite = row.getString(1);
        String dateStr = row.getString(2);
        String detectorNum = row.getString(3);
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Date date = df.parse(dateStr.split(" ")[0]);
        String dayOfWeek_Str = new SimpleDateFormat("EE").format(date).toString();

        builder.reset();
        String fid = scatsSite + "-" + detectorNum + "-" + date.getTime();
        SimpleFeature simpleFeature = builder.buildFeature(fid);
        // Tell GeoMesa to use user provided FID
        simpleFeature.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);
        simpleFeature.setAttribute("geometry", point);
        simpleFeature.setAttribute("NB_SCATS_SITE", scatsSite);
        simpleFeature.setAttribute("QT_INTERVAL_COUNT", date);
        simpleFeature.setAttribute("NB_DETECTOR", detectorNum);
        simpleFeature.setAttribute("DAY_OF_WEEK", dayOfWeek_Str);
        for (int i = 0; i < 96; i++) {
            String value = row.getString(i + 4);
            String feature = i < 10 ? "V0" + Integer.toString(i) : "V" + Integer.toString(i);
            simpleFeature.setAttribute(feature, value);
        }
        simpleFeature.setAttribute("DS_LOCATION", row.getString(100));
        simpleFeature.setAttribute("NB_LANE", row.getString(101));
        simpleFeature.setAttribute("LANE_MVT", row.getString(102));
        simpleFeature.setAttribute("LOC_MVT", row.getString(103));
        simpleFeature.setAttribute("HF", row.getString(104));
        simpleFeature.setAttribute("unique_road", line);
        return simpleFeature;
    }

    public static SimpleFeature buildFeatureFromRow(Row row) throws ParseException, SchemaException, FactoryException {
        SimpleFeatureBuilder builder = getFeatureBuilder();
        return buildFeatureFromRow(row, builder);
    }

    /**
     * Not a suggested way to build simple feature type for GeoMesa API
     *
     * @param simpleFeatureTypeName
     * @return
     * @throws SchemaException
     * @throws FactoryException
     */
    @Deprecated
    public static SimpleFeatureType createSimpleFeatureType_depr(String simpleFeatureTypeName)
            throws SchemaException, FactoryException {
        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
        builder.setName(simpleFeatureTypeName);
        CoordinateReferenceSystem targetCRS = CRS.decode("EPSG:4326");
        builder.setCRS(targetCRS);
        builder.add("geometry", Point.class);
        builder.add("NB_SCATS_SITE", String.class);
        builder.add("QT_INTERVAL_COUNT", Date.class);
        builder.add("NB_DETECTOR", String.class);
        for (int i = 0; i < 96; i++) {
            String attribute;
            if (i < 10)
                attribute = "V0" + Integer.toString(i);
            else
                attribute = "V" + Integer.toString(i);
            builder.add(attribute, String.class);
        }
        builder.add("DS_LOCATION", String.class);
        builder.add("NB_LANE", String.class);
        builder.add("LANE_MVT", String.class);
        builder.add("LOC_MVT", String.class);

        SimpleFeatureType simpleFeatureType = builder.buildFeatureType();
        simpleFeatureType.getUserData().put(Constants.SF_PROPERTY_START_TIME, "QT_INTERVAL_COUNT");
        return simpleFeatureType;
    }
}
