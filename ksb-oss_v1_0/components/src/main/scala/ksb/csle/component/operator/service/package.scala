package ksb.csle.component.operator

package object service {
  case class WeatherRequest(baseUrl: String, serviceKey: String, time: String, nx: String, ny: String)
  case class StrokeIndexRequest(baseUrl: String, serviceKey: String, time: String, coord: String)
}
