package scats.utils.accumulo;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.geotools.data.DataStoreFinder;
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore;
import org.opengis.feature.simple.SimpleFeatureType;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Yikai Gong
 */

public class AccumuloTool {
  private static final String INSTANCE_ID = "instanceId";
  private static final String ZOOKEEPERS = "zookeepers";
  private static final String USER = "user";
  private static final String PASSWORD = "password";
  private static final String AUTHS = "auths";
  private static final String TABLE_NAME = "tableName";
  private static final String FEATURE_NAME = "featureName";
  private static final String GENERATE_STATS = "generateStats";
  private static final String[] ACCUMULO_CONNECTION_PARAMS = new String[]{INSTANCE_ID,
    ZOOKEEPERS, USER, PASSWORD, TABLE_NAME};

  public static Options addAccumuloOptions(Options options) {
    if (options == null)
      options = new Options();
    Option instanceIdOpt = new Option("i", INSTANCE_ID, true, "accumulo connection parameter instanceId");
    instanceIdOpt.setRequired(true);
    Option zookeepersOpt = new Option("z", ZOOKEEPERS, true, "accumulo connection parameter zookeepers");
    zookeepersOpt.setRequired(true);
    Option userOpt = new Option("u", USER, true, "accumulo connection parameter user");
    userOpt.setRequired(true);
    Option passwordOpt = new Option("p", PASSWORD, true, "accumulo connection parameter password");
    passwordOpt.setRequired(true);
    Option tableNameOpt = new Option("tb", TABLE_NAME, true, "accumulo connection parameter tableName");
    tableNameOpt.setRequired(true);
    Option authsOpt = new Option("au", AUTHS, true, "accumulo connection parameter auths");
    Option featureNameOpt = new Option("fn", FEATURE_NAME, true, "name of feature in accumulo table");

    options.addOption(instanceIdOpt);
    options.addOption(zookeepersOpt);
    options.addOption(userOpt);
    options.addOption(passwordOpt);
    options.addOption(authsOpt);
    options.addOption(tableNameOpt);
    options.addOption(featureNameOpt);
    return options;
  }

  public static Map<String, String> getAccumuloDataStoreConf(CommandLine cmd) {
    Map<String, String> dsConf = new HashMap<>();
    for (String param : ACCUMULO_CONNECTION_PARAMS) {
      dsConf.put(param, cmd.getOptionValue(param));
    }
    return dsConf;
  }

  public static void flagGenerateStats(Map<String, String> dsConf, Boolean flag) {
    dsConf.put(GENERATE_STATS, flag.toString());
  }

  public static void saveSimpleFeatureType(Map dsConf, SimpleFeatureType simpleFeatureType) {
    try {
      AccumuloDataStore dataStore
        = (AccumuloDataStore) DataStoreFinder.getDataStore(dsConf);
      dataStore.createSchema(simpleFeatureType);
      if (dataStore != null) {
        dataStore.dispose();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
