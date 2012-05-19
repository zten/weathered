package weathered.couch

import reflect.BeanProperty
import org.codehaus.jackson.annotate.JsonProperty

/**
 * station object
 *
 * @author Christopher Childs
 * @version 1.0, 5/18/12
 */

class Station {
  @JsonProperty("_id")
  @BeanProperty
  var id = ""

  @JsonProperty("_rev")
  @BeanProperty
  var revision = ""

  @BeanProperty
  var usaf = 0

  @BeanProperty
  var wban = 0

  @BeanProperty
  var name = ""

  @BeanProperty
  var country = ""

  @BeanProperty
  var fipsCountry = ""

  @BeanProperty
  var state = ""

  @BeanProperty
  var callsign = ""

  @BeanProperty
  var elevation = ""
}
