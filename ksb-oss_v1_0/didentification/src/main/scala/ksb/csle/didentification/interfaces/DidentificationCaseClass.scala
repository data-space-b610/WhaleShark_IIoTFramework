package ksb.csle.didentification.interfaces

import scala.collection.mutable.Map

/**
 * This case class contains the interval information [lower, upper].
 * 
 * @param lower the lower bound of this interval
 * @param upper the upper bound of this interval
 */
case class Interval(lower: Double, upper: Double)

/**
 * This case class contains the outlier information [lower, upper].
 * The given value is beyond [lower, upper], it is considered as outlier,
 * which may be replaced by the 'replace' value. 
 * 
 * @param lower the lower bound of this interval
 * @param upper the upper bound of this interval
 */
case class OutlierInfo(lower: Double, upper: Double, replace: String)

/**
 * The hierarchical information is changed by how many times the algorithm
 * scrubs (or generalizes) the original data. This case class describes this
 * information of a column with respect to the number (or depth) of scrubbing
 * the data. [depth, original data => generalized data w.r.t. depth].
 * 
 * @param depth the number of scrubbing the data
 * @param hierarchyEntry the information containing the hierarchical info
 */
case class HierarchyEntries(depth: Int, hierarchyEntry: Map[String, String])
    
/**
 * This case class contains the list of hierarchical information of
 * a column as a map [the number of generalization (depth), generalized
 * data w.r.t. to depth].
 * 
 * @param hierarhcyDepth the information containing the hierarchical info
 * w.r.t. the given depth
 */
case class ColumnHierarchy(hierarhcyDepth: Map[Int, HierarchyEntries])


/**
 * This case class indicates how many times to scrub the column
 * 
 * @param columnIndex the index of column
 * @param columnName the name of column
 * @param currentDepth how many times to scrub the column. i.e., currentDepth = 1
 * means that it scrub the column one time
 * @param maxDepth the possible number of scrub 
 * @param doStop continuously scrub the column or not. For example, if it is
 * sufficient to satisfy the privacy policy by only scrubing one time, scrubbing 
 * the column two times is useless because it surely satisfy the privacy policy
 */
case class LatticeValue(
    columnIndex: Int,
    columnName: String,
    currentDepth: Int,
    maxDepth: Int,
    doStop: Boolean)

/**
 * The lattice value indicates how to scrub the specific single column. 
 * Generally, there are many quasi-identifiers in the data set, so it may 
 * be required to manage those quasi-identifiers. Lattice entry manages them.
 *   
 * @param entry the list of lattice value 
 * @param loss the measured information loss when this lattice entry applies
 * @param risk the measured de-identification risk when this lattice entry applies
 */
case class LatticeEntry(
    entry: List[LatticeValue],
    anonymity: Double,
    loss: Double,
    risk: Double)
    
/**
 * This case class manages the information about the equivalence class.
 * It contains its sequence of quasi-identifiers, the number of records, 
 * and its lattice entry to get this equivalence class.
 *   
 * @param eqIdentifiers The sequence of quasi-identifiers in this EC
 * @param count The number of records in this EC
 * @param latticeEntry The lattice entry to get this EC
 */
case class EquivalenceClass(
    eqIdentifiers: Seq[Any],
    count: Long,
    latticeEntry: List[LatticeValue])

/**
 * This case class provides some information about the loss occurred by
 * the de-identification.
 *   
 * @param loss Pure information loss, but not normalized
 * @param lowerBound The lower bound of pure information loss
 * @param relativeLoss Relative loss to original data
 * @param relativeLowerBound Relative lower bound of loss
 */
case class InformationLossBound(
    loss: Double, 
    lowerBound: Double, 
    relativeLoss: Double,
    relativelowerBound: Double)

