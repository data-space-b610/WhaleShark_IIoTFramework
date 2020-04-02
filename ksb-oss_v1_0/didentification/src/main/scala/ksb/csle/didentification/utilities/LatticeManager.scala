package ksb.csle.didentification.utilities

import scala.collection.mutable.Map
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.StreamDidentProto._

import ksb.csle.didentification.interfaces._
/**
 * This trait manages lattice entry information. Lattice entry indicates
 * how the algorithm generalizes the columns of quasi-identifiers. For example,
 * if the quasi-identifier is column 1, 2, and 3, the lattice manager manages
 * three lattice values where each lattice value indicate
 */
object LatticeManager {
  
  /**
   * Makes the list of lattice entries based on the hierarchy information
   * 
   * @param hierarchies the information of columns and their hierarchies
   * @return List[LatticeEntry] the list of lattice entries
   */
  def makeLatticeEntries(
      hierarchies: Map[FieldInfo, ColumnHierarchy])
      : List[LatticeEntry] = {
    var latticeSet = List[LatticeEntry]()
    latticeSet = latticeSet :+ getInitLatticeEntry(hierarchies)
    
    hierarchies.map(hierarchy => {
      latticeSet = expandLatticeEntry(latticeSet,
        hierarchy._1.getKey.toInt, 
        depthOfHierarchy(hierarchy._2))
    })
    (latticeSet)
  }
  
  /**
   * Sorts the lattice entries by the weight (currentDepth / maxDepth)
   * 
   * @param latticeSet the lattice entries to be sorted
   * @return List[LatticeEntry] the sorted lattice entries
   */
  def sortLatticeSet(
      latticeSet: List[LatticeEntry])
  : List[LatticeEntry] = 
    latticeSet.sortBy(latticeEntry => {
      var weight = 0.0
      latticeEntry.entry.map(latticeValue => 
        weight += latticeValue.currentDepth / latticeValue.maxDepth)
      weight
    })
    
  private def getInitLatticeEntry(
      hierarchies: Map[FieldInfo, ColumnHierarchy])
  : LatticeEntry = {
    var latticeEntry = List[LatticeValue]()
    hierarchies.map(hierarchy => 
      latticeEntry = latticeEntry :+ 
        new LatticeValue(hierarchy._1.getKey.toInt, 
            hierarchy._1.getValue, 0, 
            depthOfHierarchy(hierarchy._2), false))

    new LatticeEntry(latticeEntry, 0.0, 0.0, 0.0)
  }
        
  private def expandLatticeEntry(
      latticeSet: List[LatticeEntry],
      colIndex: Int,
      maxDepth: Int): List[LatticeEntry] = {
    var expandLatticeSet = List[LatticeEntry]()
    latticeSet.map(latticeEntry => {
      for(i <- 0 to maxDepth) {
        latticeEntry.entry.indices.map(j => 
          if(latticeEntry.entry(j).columnIndex == colIndex) {
            val index = j
            val newEntry = latticeEntry.entry(index).copy(currentDepth = i)
            expandLatticeSet = expandLatticeSet :+ new LatticeEntry(
              latticeEntry.entry.updated(index, newEntry), 0.0, 0.0, 0.0)
          })
      }
    })
    
    (expandLatticeSet)
  }
  
  /**
   * Returns the depth of given column hierarchy
   * 
   * @param hierarchy the hierarchy information
   * @return Int the depth of column hierarchy
   */
  def depthOfHierarchy(hierarchy: ColumnHierarchy): Int =
    hierarchy.hierarhcyDepth.size
 
  def printLatticeEntries(entries: List[LatticeEntry]): Unit = {
    entries.map(entry => printLatticeEntry(entry))
  }
  
  def printLatticeEntry(entry: LatticeEntry): Unit = {
    print("\nLattice Entry: ")
    entry.entry.map(value => print(value.currentDepth + " "))
   
    entry.entry.map(value => 
      print("\n\tColumn: " + value.columnName + "\tCurrent Hierarhcy Depth: (" +
          value.currentDepth + "/" + value.maxDepth + ")")
    )
    print("\n\tData Loss: " + entry.loss + "\tRisk: " + entry.risk + "\n")      
  }
  
}