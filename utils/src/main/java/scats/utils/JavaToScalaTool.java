package scats.utils;

/**
 * @author Yikai Gong
 */

import scala.collection.JavaConverters$;
import scala.collection.immutable.Map;

public class JavaToScalaTool {

  public static <K, V> Map<K, V> convertJavaMapToScalaMap(java.util.Map<K, V> m) {
    return JavaConverters$.MODULE$.mapAsScalaMapConverter(m).asScala().toMap(
      scala.Predef$.MODULE$.<scala.Tuple2<K, V>>$conforms()
    );
  }
}
