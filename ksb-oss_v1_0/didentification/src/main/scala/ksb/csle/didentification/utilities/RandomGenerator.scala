package ksb.csle.didentification.utilities

/**
 * This object provides some functions to make random strings.
 */
object RandomGenerator {
  
  /**
   * Makes a single random upper case alphabet  
   * @return String.
   */  
  def randomAlphaUpper(): String = randomAlphaUpper(1)
  
  /**
   * Makes random upper case alphabets
   * @param length the length of random alphabet to generate  
   * @return String.
   */  
  def randomAlphaUpper(length: Int): String = {
    val chars = ('A' to 'Z')
    randomStringFromCharList(length, chars)
  }
  
  /**
   * Makes a single random lower case alphabet  
   * @return String.
   */  
  def randomAlphaLower(): String = randomAlphaLower(1)
  
  /**
   * Makes random lower case alphabets
   * @param length the length of random alphabet to generate  
   * @return String.
   */  
  def randomAlphaLower(length: Int): String = {
    val chars = ('a' to 'z')
    randomStringFromCharList(length, chars)
  }
  
  /**
   * Makes a single random alphabet  
   * @return String.
   */  
  def randomAlpha(): String = randomAlpha(1)
  
  /**
   * Makes random alphabets
   * @param length the length of random alphabet to generate  
   * @return String.
   */  
  def randomAlpha(length: Int): String = {
    val chars = ('a' to 'z') ++ ('A' to 'Z')
    randomStringFromCharList(length, chars)
  }
  
  /**
   * Makes a single random number  
   * @return Integer.
   */  
  def randomNumber(): Int = randomNumber(1)
  
  /**
   * Makes random numbers
   * @param length the length of random numbers to generate  
   * @return String.
   */  
  def randomNumber(length: Int): Int = {
    val chars = ('0' to '9')
    randomStringFromCharList(length, chars).toInt
  }
  
  /**
   * Makes a single random string  
   * @return String.
   */  
  def randomMixed(): String = randomMixed(1)
  
  /**
   * Makes random string
   * @param length the length of random string to generate  
   * @return String.
   */  
  def randomMixed(length: Int): String = {
    val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')
    randomStringFromCharList(length, chars)
  }
  
  private def randomStringFromCharList(length: Int, chars: Seq[Char]): String = {
    val sb = new scala.collection.mutable.StringBuilder
    for (i <-1 to length) {
      val randomNum = scala.util.Random.nextInt(chars.length)
      sb.append(chars(randomNum))
    }
    sb.toString()
  }  
  
}