package weathered;

/**
 * Entry point for the most fantabulous ISD lite parsing and indexing app ever.
 * Or maybe it's not the most fantabulous.
 *
 */
object Weathered {
  def main(args:Array[String]) {
    val test = "2012 01 01 00    17    12  9661   100    60     8 -9999     2"
    val list = test.split("\\s+").map(s => Integer.valueOf(s))

    list.foreach(x => println(x))

  }
}
