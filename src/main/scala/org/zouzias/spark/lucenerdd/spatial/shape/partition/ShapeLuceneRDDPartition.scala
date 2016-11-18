/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.zouzias.spark.lucenerdd.spatial.shape.partition

import com.spatial4j.core.shape.Shape
import org.apache.lucene.document.{Document, StoredField}
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.IndexSearcher
import org.joda.time.DateTime
import org.zouzias.spark.lucenerdd.query.LuceneSpatialQueryHelpers
import org.zouzias.spark.lucenerdd.response.LuceneRDDResponsePartition
import org.zouzias.spark.lucenerdd.spatial.shape.ShapeLuceneRDD.PointType
import org.zouzias.spark.lucenerdd.store.IndexWithTaxonomyWriter

import scala.reflect._

private[shape] class ShapeLuceneRDDPartition[K, V]
  (private val iter: Iterator[(K, V)])
  (override implicit val kTag: ClassTag[K],
   override implicit val vTag: ClassTag[V])
  (implicit shapeConversion: K => Shape,
   docConversion: V => Document)
  extends AbstractShapeLuceneRDDPartition[K, V] with IndexWithTaxonomyWriter {

  private val (iterOriginal, iterIndex) = iter.duplicate

  private val startTime = new DateTime(System.currentTimeMillis())
  logInfo(s"Indexing process initiated at ${startTime}...")
  iterIndex.foreach { case (key, value) =>
    // (implicitly) convert type K to Shape and V to a Lucene document
    val doc = docConversion(value)
    val shape = shapeConversion(key)
    val docWithLocation = LuceneSpatialQueryHelpers.decorateWithLocation(doc, Seq(shape))
    indexWriter.addDocument(FacetsConfig.build(taxoWriter, docWithLocation))
  }
  private val endTime = new DateTime(System.currentTimeMillis())
  logInfo(s"Indexing process completed at ${endTime}...")
  logInfo(s"Indexing process took ${(endTime.getMillis - startTime.getMillis) / 1000} seconds...")

  // Close the indexWriter and taxonomyWriter (for faceted search)
  closeAllWriters()

  private val indexReader = DirectoryReader.open(IndexDir)
  private val indexSearcher = new IndexSearcher(indexReader)

  override def size: Long = iterOriginal.size.toLong

  /**
   * Restricts the entries to those satisfying a predicate
   *
   * @param pred
   * @return
   */
  override def filter(pred: (K, V) => Boolean): AbstractShapeLuceneRDDPartition[K, V] = {
    ShapeLuceneRDDPartition(iterOriginal.filter(x => pred(x._1, x._2)))
  }

  override def isDefined(key: K): Boolean = iterOriginal.exists(_._1 == key)

  override def iterator: Iterator[(K, V)] = iterOriginal

  override def circleSearch(center: PointType, radius: Double, k: Int, operationName: String)
  : LuceneRDDResponsePartition = {
    logInfo(s"circleSearch [center:${center}, operation:${operationName}]")
    val docs = LuceneSpatialQueryHelpers
      .circleSearch(center, radius, k, operationName)(indexSearcher)
    LuceneRDDResponsePartition(docs)
  }

  override def knnSearch(point: PointType, k: Int, searchString: String)
  : LuceneRDDResponsePartition = {
    logInfo(s"knnSearch [center:${point}, searchQuery:${searchString}]")
    val docs = LuceneSpatialQueryHelpers.knnSearch(point, k, searchString)(indexSearcher)
    LuceneRDDResponsePartition(docs)
  }

  override def spatialSearch(shapeAsString: String, k: Int, operationName: String)
  : LuceneRDDResponsePartition = {
    logInfo(s"spatialSearch [shape:${shapeAsString} and operation:${operationName}]")
    val docs = LuceneSpatialQueryHelpers
      .spatialSearch(shapeAsString, k, operationName)(indexSearcher)
    LuceneRDDResponsePartition(docs.toIterator)
  }

  override def spatialSearch(point: PointType, k: Int, operationName: String)
  : LuceneRDDResponsePartition = {
    val docs = LuceneSpatialQueryHelpers.spatialSearch(point, k, operationName)(indexSearcher)
    LuceneRDDResponsePartition(docs.toIterator)
  }

  override def bboxSearch(center: PointType, radius: Double, k: Int, operationName: String)
  : LuceneRDDResponsePartition = {
    val docs = LuceneSpatialQueryHelpers.bboxSearch(center, radius, k, operationName)(indexSearcher)
    LuceneRDDResponsePartition(docs.toIterator)
  }

  override def bboxSearch(lowerLeft: PointType, upperRight: PointType, k: Int, opName: String)
  : LuceneRDDResponsePartition = {
    val docs = LuceneSpatialQueryHelpers.bboxSearch(lowerLeft, upperRight, k, opName)(indexSearcher)
    LuceneRDDResponsePartition(docs.toIterator)
  }
}

object ShapeLuceneRDDPartition {

  def apply[K: ClassTag, V: ClassTag](iter: Iterator[(K, V)])
  (implicit shapeConv: K => Shape, docConv: V => Document)
  : ShapeLuceneRDDPartition[K, V] = {
    new ShapeLuceneRDDPartition[K, V](iter) (classTag[K], classTag[V]) (shapeConv, docConv)
  }
}
