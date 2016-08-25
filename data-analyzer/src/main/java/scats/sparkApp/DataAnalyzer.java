package scats.sparkApp;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.Query;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore;
import org.locationtech.geomesa.compute.spark.GeoMesaSpark;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import scats.utils.JavaToScalaTool;
import scats.utils.MethodTimer;
import scats.utils.accumulo.AccumuloTool;
import scats.utils.geotools.factory.ScatsDOWFeatureFactory;
import scats.utils.spark.SparkAccumuloDataStore;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author Yikai Gong
 */

public class DataAnalyzer {
    private static final String APP_NAME = "ScatsDataAnalyzer";
    private static Logger logger = LoggerFactory.getLogger(DataAnalyzer.class);
    private static int timeSlotsNum = 96;
    private static CommandLine cmd;

    private SparkConf sparkConf;

    public DataAnalyzer() {
        this(new SparkConf());
    }

    public DataAnalyzer(SparkConf sparkConf) {
        this.sparkConf = sparkConf.setAppName(APP_NAME);
    }

    public static void main(String[] args) {
        try {
            cmd = getCmd(args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        DataAnalyzer dataAnalyzer = new DataAnalyzer();
        MethodTimer.print(() -> {dataAnalyzer.run(cmd); return null;});
        System.exit(0);
    }

    private void run(CommandLine cmd) {
        Map<String, String> dsConf = AccumuloTool.getAccumuloDataStoreConf(cmd);
        AccumuloTool.flagGenerateStats(dsConf, Boolean.FALSE);
        AccumuloDataStore dataStore = null;
        Query query = null;
        try {
            // Init feature type in GeoMesa/Accumulo
            AccumuloTool.saveSimpleFeatureType(dsConf, ScatsDOWFeatureFactory.createFeatureType());
            // Prepare dataStore and query for creating RDD<SimpleFeature>
            dataStore = (AccumuloDataStore) DataStoreFinder.getDataStore(dsConf);
            query = new Query("ScatsVolumeData"); //, CQL.toFilter("DAY_OF_WEEK='Tue'")
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        sparkConf = GeoMesaSpark.init(sparkConf, dataStore);
        SparkContext sparkContext = new SparkContext(sparkConf);
        JavaSparkContext jsc = new JavaSparkContext(sparkContext);

        // Load JavaRDD<SimpleFeature> scatsVolume
        scala.Option<Object> none = scala.Option.apply(null);
        RDD<SimpleFeature> queryRDD
                = GeoMesaSpark.rdd(new Configuration(), sparkContext,
                JavaToScalaTool.convertJavaMapToScalaMap(dsConf), query, none);
        JavaRDD<SimpleFeature> queryJavaRDD =
                JavaRDD.fromRDD(queryRDD, scala.reflect.ClassTag$.MODULE$.apply(SimpleFeature.class));


        JavaPairRDD<String, double[]> rddResult = queryJavaRDD.mapToPair(simpleFeature -> {
            String scatsSite = (String) simpleFeature.getAttribute("NB_SCATS_SITE");
            String detectorNum = (String) simpleFeature.getAttribute("NB_DETECTOR");
            String dayOfWeek = (String) simpleFeature.getAttribute("DAY_OF_WEEK");
            String geo_wkt = (simpleFeature.getDefaultGeometry()).toString();
            String key = scatsSite + "#" + detectorNum + "#" + dayOfWeek + "#" + geo_wkt;

            int[] value = new int[timeSlotsNum + 1];
            for (int i = 0; i < timeSlotsNum; i++) {
                String attribute = i < 10 ? "V0" + Integer.toString(i) : "V" + Integer.toString(i);
                value[i] = Integer.parseInt((String) simpleFeature.getAttribute(attribute));
            }
            value[value.length - 1] = 1;  // feature counter
            return new Tuple2<>(key, value);
        }).reduceByKey((a1, a2) -> {
            int[] result = new int[timeSlotsNum + 1];
            for (int i = 0; i < result.length; i++) {
                result[i] = a1[i] + a2[i];
            }
            return result;
        }).mapToPair(tuple -> {
            double[] result = new double[timeSlotsNum + 1];
            for (int i = 0; i < result.length; i++) {
                result[i] = tuple._2[i] / (double) tuple._2[tuple._2.length - 1];
            }
            result[result.length - 1] = (double) tuple._2[tuple._2.length - 1];
            return new Tuple2<>(tuple._1, result);
        });

        rddResult.foreachPartition(tuple2Iterator -> {
            SparkAccumuloDataStore.lazyInit(dsConf, ScatsDOWFeatureFactory.FT_NAME);
            SimpleFeatureBuilder builder = ScatsDOWFeatureFactory.getFeatureBuilder();
            List<SimpleFeature> features =
                    ScatsDOWFeatureFactory.buildFeaturesFromTuple(tuple2Iterator.next(), builder);
            SparkAccumuloDataStore.writeFeatures(features);
        });

        List<Integer> data = Arrays.asList(1, 2, 3);
        JavaRDD<Integer> distData = jsc.parallelize(data, 100);
        distData.foreachPartition(integerIterator -> {
            SparkAccumuloDataStore.close();
        });
        sparkContext.stop();
    }

    private static Options getRequiredOptions() {
        Options options = AccumuloTool.addAccumuloOptions(new Options());
        return options;
    }

    private static CommandLine getCmd(String[] args) throws ParseException {
        CommandLineParser parser = new BasicParser();
        Options options = getRequiredOptions();
        return parser.parse(options, args);
    }
}
