package ksb.csle.didentification.interfaces

import ksb.csle.common.proto.StreamDidentProto._

/**
 * This trait provides the function to translate the given specific
 * methods to related strings. 
 */
trait MethodString {
  
  def getMethodString[T](method: T): String = 
    method match {
      case RandomMethod.ALPHABET => "ALPHABET"
      case RandomMethod.NUMBER => "NUMBER"
      case RandomMethod.MIXED => "MIXED"
      case AggregationMethod.MIN => "MIN"
      case AggregationMethod.MAX => "MAX"
      case AggregationMethod.AVG => "AVG"
      case AggregationMethod.STD => "STD"
      case AggregationMethod.COUNT => "COUNT"
      case RoundingMethod.ROUND => "ROUND"
      case RoundingMethod.ROUND_DOWN => "ROUND_DOWN"
      case RoundingMethod.ROUND_UP => "ROUND_UP"
      case ReplaceValueMethod.BLANK => "BLANK"
      case ReplaceValueMethod.STAR => "STAR"
      case ReplaceValueMethod.UNDERBAR => "UNDERBAR"
      case _ => "MIXED"
    }

}
