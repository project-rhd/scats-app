package scats.sparkApp;

import com.google.common.base.Joiner;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.SizeEstimator;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.FactoryException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scats.sparkApp.dataFramesSchema.SiteLayouts;
import scats.utils.MethodTimer;
import scats.utils.RawVolumeDataCleaner;
import scats.utils.accumulo.AccumuloTool;
import scats.utils.geotools.factory.ScatsFeatureFactory;
import scats.utils.spark.SparkAccumuloDataStore;

import java.io.IOException;
import java.util.*;

/**
 * @author Yikai Gong
 */

public class DataImporter {
    private static final String APP_NAME = "ScatsDataImporter";
    private static final String INPUT_VOLUME_FILE = "inputVolumeFile";
    private static final String INPUT_LAYOUT_FILE = "inputLayoutFile";
    private static final String INPUT_SHAPE_FILE = "inputShapeFile";
    private static Logger logger = LoggerFactory.getLogger(DataImporter.class);

    private static CommandLine cmd;
    private SparkConf sparkConf;

    // Constructors
    public DataImporter() {
        this(new SparkConf());
    }

    public DataImporter(SparkConf sparkConf) {
        this.sparkConf = sparkConf.setAppName(APP_NAME);
        // Use Kryo serialization
        this.sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        Class[] classes = new Class[]{HashMap.class, String.class, Row.class};
        this.sparkConf.registerKryoClasses(classes);
    }

    public static void main(String[] args) {
        try {
            cmd = getCmd(args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        DataImporter dataImporter = new DataImporter();
        MethodTimer.print(() -> {dataImporter.run(cmd); return null;});
        System.exit(0);
    }

    private void run(CommandLine cmd) {
        // Init Spark Context
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        SparkSession sparkSession = new SparkSession(sparkContext.sc());

        // Configures the number of partitions to use when shuffling data for joins or aggregations.
        // 5000 works fine
        sparkSession.sqlContext().setConf("spark.sql.shuffle.partitions", "200"); // 5000 200

        // Create feature type schemes in accumulo
        Map<String, String> dsConf = AccumuloTool.getAccumuloDataStoreConf(cmd);
        // To avoid a runtime error which is a known bug in GeoMesa 1.2.3 with Spark
        AccumuloTool.flagGenerateStats(dsConf, Boolean.FALSE);
        SimpleFeatureType sft = null;
        try {
            sft = ScatsFeatureFactory.createFeatureType();
        } catch (SchemaException|FactoryException e) {
            e.printStackTrace();
        }
        AccumuloTool.saveSimpleFeatureType(dsConf, sft);

        // Read shape file from hdfs and parse into a hashmap
        String path = cmd.getOptionValue(INPUT_SHAPE_FILE);
        HashMap<String, String> scatsShp = readWktCsv("hdfs://scats-1-master:9000", path);
        Broadcast<HashMap> scatsShp_b = sparkContext.broadcast(scatsShp);

        // 1. Process volume data
        long size = SizeEstimator.estimate(sparkContext.textFile(cmd.getOptionValue(INPUT_VOLUME_FILE)));
        System.out.println("Estimated size of rawVolumeData is " + size);
        JavaRDD<String> rawVolumeData = sparkContext.textFile(cmd.getOptionValue(INPUT_VOLUME_FILE), 100); //15000 100

        // Map volumeData from RDD<String> to RDD<Row> and add geo location to each row
        String volumeData_head = rawVolumeData.first();
        JavaRDD<Row> volumeDataRDD = rawVolumeData.mapPartitionsWithIndex((idx, iter) -> {
            // Skip file header (first line of partition_1)
            if (idx == 0)
                iter.next();
            ArrayList<Row> result = new ArrayList<>();
            while (iter.hasNext()) {
                String line = iter.next();
                String cleanedLine = RawVolumeDataCleaner.cleanScatsTupleStr(line);
                if (cleanedLine != null) {
                    String[] fields = cleanedLine.split(",");
                    String nb_scats_site = fields[0];
                    String wktGeometry = (String) scatsShp_b.value().get(nb_scats_site);
                    if(wktGeometry != null){
                        cleanedLine = wktGeometry + "," + Joiner.on(",").join(fields);
                        fields = cleanedLine.split(",");
                        result.add(RowFactory.create(fields));
                    }
                }
            }
            return result.iterator();
        }, false);

        // Convert volumeData from RDD<Row> to Dataset<Row>
        List<StructField> volume_field_names = new ArrayList<>();
        volume_field_names.add(DataTypes.createStructField("wktGeometry", DataTypes.StringType, true));
        for (String fieldName : volumeData_head.split(",")) {
            fieldName = fieldName.substring(1, fieldName.length() - 1);
            volume_field_names.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
        }
        StructType VolumeDataSchema = DataTypes.createStructType(volume_field_names);
        Dataset<Row> schemaVolumeData = sparkSession.createDataFrame(volumeDataRDD, VolumeDataSchema);
        schemaVolumeData.createOrReplaceTempView("volumeData");

        // 2. Process siteLayOut data
        long size_2 = SizeEstimator.estimate(sparkContext.textFile(cmd.getOptionValue(INPUT_LAYOUT_FILE)));
        System.out.println("Estimated size of siteLayoutsStr is " + size_2);
        JavaRDD<String> siteLayoutsStr = sparkContext.textFile(cmd.getOptionValue(INPUT_LAYOUT_FILE), 200); //4000 200
//        siteLayoutsStr.persist(StorageLevel.DISK_ONLY());

        // Map layoutData from RDD<String> to RDD<SiteLayouts>
        JavaRDD<SiteLayouts> siteLayoutsRDD = siteLayoutsStr.mapPartitionsWithIndex((idx, iter) -> {
            if (idx == 0)
                iter.next();
            List<SiteLayouts> result = new ArrayList<>();
            while (iter.hasNext()) {
                String[] fields = RawVolumeDataCleaner.removeQuotation(iter.next().split(","));
                result.add(new SiteLayouts(fields[0], fields[3], fields[4],
                        fields[5], fields[6], fields[7], fields[8]));
            }
            return result.iterator();
        }, false);

        // Convert layoutData from RDD<SiteLayouts> to Dataset<Row>
        Dataset<Row> schemaSiteLayouts = sparkSession.createDataFrame(siteLayoutsRDD, SiteLayouts.class);
        schemaSiteLayouts.createOrReplaceTempView("siteLayouts");

        // 3. SQL operations on Dataset
        String sql = "SELECT b.*, " +
                "c.DS_LOCATION, c.NB_LANE, c.LANE_MVT, c.LOC_MVT " +
                "FROM volumeData b " +
                "LEFT JOIN siteLayouts c " +
                "ON b.NB_SCATS_SITE = c.NB_SCATS_SITE " +
                "AND b.NB_DETECTOR = c.NB_DETECTOR " +
                "AND b.QT_INTERVAL_COUNT = c.DT_GENERAL";

        Dataset<Row> scatsData = sparkSession.sql(sql);
        scatsData.createOrReplaceTempView("scats");
        scatsData.persist(StorageLevel.MEMORY_AND_DISK_SER());

        scatsData.foreachPartition(iterator -> {
            if (!iterator.hasNext())
                return;
            SimpleFeatureBuilder featureBuilder = ScatsFeatureFactory.getFeatureBuilder();
            SparkAccumuloDataStore.lazyInit(dsConf, ScatsFeatureFactory.FT_NAME);
            while (iterator.hasNext()) {
                Row row = iterator.next();
                SimpleFeature simpleFeature = ScatsFeatureFactory.buildFeatureFromRow(row, featureBuilder);
                SparkAccumuloDataStore.write(simpleFeature);
            }
        });

        // Close db connection and flush data.
        List<Integer> data = Arrays.asList(1, 2, 3);
        JavaRDD<Integer> distData = sparkContext.parallelize(data, 100);
        distData.foreachPartition(integerIterator -> {
            SparkAccumuloDataStore.close();
        });
        sparkContext.close();
    }

    private static Options getRequiredOptions() {
        Options options = AccumuloTool.addAccumuloOptions(new Options());
        // Input files parameters
        Option inputVolumeFileOption = new Option("inv", INPUT_VOLUME_FILE, true, "path to raw volume data for processing");
        inputVolumeFileOption.setRequired(true);
        Option inputLayoutFileOption = new Option("inl", INPUT_LAYOUT_FILE, true, "path to site layout data for processing");
        inputLayoutFileOption.setRequired(true);
        Option inputShapeFileOption = new Option("ins", INPUT_SHAPE_FILE, true, "path to shape data for processing");
        inputShapeFileOption.setRequired(true);
        options.addOption(inputVolumeFileOption);
        options.addOption(inputLayoutFileOption);
        options.addOption(inputShapeFileOption);
        return options;
    }

    private static CommandLine getCmd(String[] args) throws ParseException {
        CommandLineParser parser = new BasicParser();
        Options options = getRequiredOptions();
        return parser.parse(options, args);
    }

    private static HashMap<String, String> readWktCsv(String hdfsSource, String path) {
        HashMap<String, String> scatsShp = new HashMap();
        try {
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS", hdfsSource);
            FileSystem fs = FileSystem.get(configuration);
            FSDataInputStream inputStream = fs.open(new Path(path));
            Scanner scanner = new Scanner(inputStream);
            // Skip header
            scanner.nextLine();
            while (scanner.hasNext()) {
                String[] fields = scanner.nextLine().split(",");
                scatsShp.put(fields[0], fields[1]);
            }
            inputStream.close();
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        assert (scatsShp.size() != 0);
        return scatsShp;
    }
}