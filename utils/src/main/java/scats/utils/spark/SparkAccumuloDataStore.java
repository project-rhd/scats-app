package scats.utils.spark;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.Transaction;
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureStore;
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author Yikai Gong
 */

public class SparkAccumuloDataStore {
    private static Logger logger = LoggerFactory.getLogger(SparkAccumuloDataStore.class);

    private static DataStore dataStore = null;
    private static AccumuloFeatureStore accumuloFeatureStore = null;
    private static AccumuloFeatureWriter accumuloFeatureWriter = null;

    /**
     * Initiate singleton objects lazily
     *
     * @param dsConf   Parameters for Accumulo Connection
     * @param typeName FeatureType name
     * @throws IOException
     */
    public static void lazyInit(Map dsConf, String typeName) throws IOException {
        if (dataStore == null) {
            try {
                dataStore = DataStoreFinder.getDataStore(dsConf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (accumuloFeatureStore == null) {
            accumuloFeatureStore = (AccumuloFeatureStore) dataStore.getFeatureSource(typeName);
        }
        if (accumuloFeatureWriter == null) {
            logger.info("Init accumuloFeatureWriter");
            accumuloFeatureWriter = accumuloFeatureStore.getDataStore().getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT);
        }
    }

    /**
     * Write features into Accumulo/GeoMesa without closing connection
     *
     * @param simpleFeatures
     * @throws IOException
     */
    public static void writeFeatures(List<SimpleFeature> simpleFeatures) throws IOException {
        for (SimpleFeature feature : simpleFeatures){
            write(feature);
        }
    }

    /**
     * Write single feature into Accumulo/GeoMesa without closing connection
     *
     * @param simpleFeature
     * @throws IOException
     */
    public static void write(SimpleFeature simpleFeature) throws IOException {
        try {
            accumuloFeatureWriter.writeToAccumulo(simpleFeature);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     *  Flush data into Accumulo
     */
    public static void flush() {
        if (accumuloFeatureWriter != null) {
            accumuloFeatureWriter.flush();
        }
    }

    /**
     * Flush Data then Close FeatureWriter and FeatureStore
     */
    public static void close() {
        if (accumuloFeatureWriter != null) {
            accumuloFeatureWriter.close();
            accumuloFeatureWriter = null;
            accumuloFeatureStore = null;
            logger.info("accumuloFeatureWriter closed");
        }
    }

    /**
     * Reset all static variables in class
     */
    public static void reset() {
        close();
        dataStore = null;
    }
}
