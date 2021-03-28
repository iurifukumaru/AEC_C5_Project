package stm

case class StopTime(trip_id: String, departure_time: String, arrival_time: String, stop_id: Int, stop_sequence: Int)
object StopTime {
  def apply(csv: String): StopTime = {
    val fields = csv.split(",", -1)
    StopTime(fields(0), fields(1), fields(2), fields(3).toInt, fields(4).toInt)
  }
}
