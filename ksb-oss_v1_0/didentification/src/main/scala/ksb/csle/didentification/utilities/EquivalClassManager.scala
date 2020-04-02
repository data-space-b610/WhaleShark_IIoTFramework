package ksb.csle.didentification.utilities

import scala.collection.mutable.Map
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ksb.csle.common.proto.DatasourceProto._

import ksb.csle.didentification.interfaces._
import ksb.csle.didentification.utilities._

/**
 * This object should be modified considerably. This object is just made
 * to verify the generalization function and syntatic anonymity (klt).
 * 
 * This object provides some functions to describe the equivalence class.
 * Note that all quasi-identifiers belonging to a specific equivalence class
 * should be same.
 */
object EquivalClassManager {
  
  /**
   * Gets the abstract information about the equivalence class (EC). The info
   * is given by form [quasi-identifiers info in EC, the number of their records]
   * 
   * @param src Dataframe 
   * @param columnNames The array of quasi-identifier columns 
   * @return DataFrame The abstract information about EC
   */
  def getECAbstractionInfo(
      src: DataFrame,
      columnNames: Array[String]): Map[Seq[Any], Long] = {
    var ecAbstractInfo = Map[Seq[Any], Long]()
    val quasiDF = src.groupBy(columnNames.map(x => col(x)): _*).count
    quasiDF.rdd.collect.map(row => {
      val quasiIdentifiers: Seq[Any] = row.toSeq.dropRight(1)
      val freqCount: Long = row.getAs[Long](row.length-1)
      
      ecAbstractInfo += (quasiIdentifiers -> freqCount)
    })
    
    ecAbstractInfo
  }
  
  /**
   * Gets the abstract information about the equivalence class (EC). The info
   * is given by form [quasi-identifiers info in EC, the number of their records]
   * 
   * @param src Dataframe 
   * @param hierarchies The information contains all the hierarchies of
   * quasi-identifier columns 
   * @return Map[Seq[Any], EquivalenceClass] Returns the map of equivalence
   * classes. Each equivalence class has its list of quasi-identifiers
   * (Seq[Any]) and EC info. The EC info is composed of [quasi-identifiers,
   * the number of its records, the lattice entry]. Note that the lattice
   * entry describes the generalization steps of quasi-identifiers to make
   * this equivalence class.
   * 
   */
  def getEquivalenceClasses(
      src: DataFrame,
      hierarchies: Map[FieldInfo, ColumnHierarchy])
    : Map[Seq[Any], EquivalenceClass] = {
    var equivalenceClasses = Map[Seq[Any], EquivalenceClass]()
    val columnNames: Array[String] = 
      AttributeManager.getQuasiColumnNames(hierarchies)
    val quasiDF = src.groupBy(columnNames.map(x => col(x)): _*).count
    quasiDF.rdd.collect.map(row => {
      val rowContent: Seq[Any] = row.toSeq.dropRight(1)
      val freqCount: Long = row.getAs[Long](row.length-1)
      
      var latticeEntry = List[LatticeValue]()
      for(i <- 0 until quasiDF.columns.length - 1) {
        val column = quasiDF.columns(i)
        hierarchies.map(hierarchy => {
          val columnID = hierarchy._1.getKey.toInt
          val columnName = hierarchy._1.getValue
          if(columnName == column) 
            latticeEntry = latticeEntry :+ getLatticeValue(columnID,  
                columnName, row.getAs(column), hierarchy._2)
        })
      }

      equivalenceClasses += (rowContent -> 
        new EquivalenceClass(rowContent, freqCount, latticeEntry))
    })
    
//    printEqClasses(equivalenceClasses)
    equivalenceClasses
  }
  
  /**
   * Prints all the equivalence classes (EC) information.
   * 
   * @param Map[Seq[Any], EquivalenceClass] the map of quasi-identifiers
   * and its equivalence class
   * @return Unit 
   * 
   */
  def printEqClasses(eqClasses: Map[Seq[Any], EquivalenceClass]): Unit = {
    eqClasses.map(eq => {
      println("Identifiers Info: " + eq._1)
      println("# of Occurance: " + eq._2.count)
      eq._2.latticeEntry.map(value => println(value))
    })
  }
  
  /**
   * Gets the lattice value from the hierarchy information using given data.
   * 
   * @param columnID The column ID
   * @param columnName The column name
   * @param data The data to get the lattice value
   * @param hierarchy the hierarchy information
   * @return LatticeValue The got lattice value
   * 
   */
  def getLatticeValue(
      columnID: Int,
      columnName: String,
      data: Any,
      hierarchy: ColumnHierarchy): LatticeValue = {
    hierarchy.hierarhcyDepth.map(entry => {
      if(entry._2.hierarchyEntry.contains(data.toString)) 
        return new LatticeValue(columnID, columnName, 
            0, LatticeManager.depthOfHierarchy(hierarchy), false)
      else if(entry._2.hierarchyEntry.values.exists(_ == data.toString))
        return new LatticeValue(columnID, columnName, 
            entry._2.depth, LatticeManager.depthOfHierarchy(hierarchy), false)
    })
    
    val depth = LatticeManager.depthOfHierarchy(hierarchy)
    new LatticeValue(columnID, columnName, depth, depth, false)
  }
  
}