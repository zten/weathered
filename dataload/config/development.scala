import com.twitter.conversions.time._
import com.twitter.logging.config._
import com.twitter.ostrich.admin.config._
import weathered.dataload.DataLoaderConfig

new DataLoaderConfig {
  admin.httpPort = 9999

}