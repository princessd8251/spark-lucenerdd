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
package org.zouzias.spark.lucenerdd.query

import com.spatial4j.core.distance.DistanceUtils
import com.spatial4j.core.shape.Shape
import org.apache.lucene.document.{Document, StoredField}
import org.apache.lucene.search.{IndexSearcher, ScoreDoc, Sort}
import org.apache.lucene.spatial.query.{SpatialArgs, SpatialOperation}
import org.zouzias.spark.lucenerdd.analyzers.LuceneAnalyzers
import org.zouzias.spark.lucenerdd.models.SparkScoreDoc
import org.zouzias.spark.lucenerdd.spatial.shape.ShapeLuceneRDD._
import org.zouzias.spark.lucenerdd.spatial.shape.strategies.SpatialStrategy

/**
  * Helper methods for Lucene Spatial queries
  */
object LuceneSpatialQueryHelpers extends SpatialStrategy {

  private def docLocation(scoreDoc: ScoreDoc)(indexSearcher: IndexSearcher): Option[Shape] = {
    val shapeString = indexSearcher.getIndexReader.document(scoreDoc.doc)
      .getField(strategy.getFieldName)
      .stringValue()

    try{
      Some(stringToShape(shapeString))
    }
    catch {
      case _: Throwable => None
    }
  }

  def decorateWithLocation(doc: Document, shapes: Iterable[Shape]): Document = {

    // Potentially more than one shape in this field is supported by some
    // strategies; see the Javadoc of the SpatialStrategy impl to see.
    shapes.foreach{ case shape =>
      strategy.createIndexableFields(shape).foreach{ case field =>
        doc.add(field)
      }

      doc.add(new StoredField(strategy.getFieldName, shapeToString(shape)))
    }

    doc
  }


  def circleSearch(center: PointType, radius: Double, k: Int, operationName: String)
    (indexSearcher: IndexSearcher): Iterator[SparkScoreDoc] = {
    val args = new SpatialArgs(SpatialOperation.get(operationName),
      ctx.makeCircle(center._1, center._2,
        DistanceUtils.dist2Degrees(radius, DistanceUtils.EARTH_MEAN_RADIUS_KM)))

    val query = strategy.makeQuery(args)
    val docs = indexSearcher.search(query, k)
    docs.scoreDocs.map(SparkScoreDoc(indexSearcher, _)).toIterator
  }

  def knnSearch(point: PointType, k: Int, searchString: String)
               (indexSearcher: IndexSearcher)
  : Iterator[SparkScoreDoc] = {

    // Match all, order by distance ascending
    val pt = ctx.makePoint(point._1, point._2)

    // the distance (in km)
    val valueSource = strategy.makeDistanceValueSource(pt, DistanceUtils.DEG_TO_KM)

    // false = ascending dist
    val distSort = new Sort(valueSource.getSortField(false)).rewrite(indexSearcher)

    val query = LuceneQueryHelpers.parseQueryString(searchString)(LuceneAnalyzers.Analyzer)
    val docs = indexSearcher.search(query, k, distSort)

    // Here we sorted on it, and the distance will get
    // computed redundantly.  If the distance is only needed for the top-X
    // search results then that's not a big deal. Alternatively, try wrapping
    // the ValueSource with CachingDoubleValueSource then retrieve the value
    // from the ValueSource now. See LUCENE-4541 for an example.
    val result = docs.scoreDocs.map { scoreDoc => {
      val location = docLocation(scoreDoc)(indexSearcher)
      location match {
        case Some(shape) =>
          SparkScoreDoc(indexSearcher, scoreDoc,
            ctx.calcDistance(pt, shape.getCenter).toFloat)
        case None =>
          SparkScoreDoc(indexSearcher, scoreDoc, -1F)
      }
    }
    }

    result.toIterator
  }

  private def spatialSearch(shape: Shape, k: Int, operationName: String)
  (indexSearcher: IndexSearcher)
  : Array[SparkScoreDoc] = {
    val args = new SpatialArgs(SpatialOperation.get(operationName), shape)
    val query = strategy.makeQuery(args)
    val docs = indexSearcher.search(query, k)
    docs.scoreDocs.map(SparkScoreDoc(indexSearcher, _))
  }


  def spatialSearch(shapeAsString: String, k: Int, operationName: String)
                   (indexSearcher: IndexSearcher)
  : Array[SparkScoreDoc] = {
    val shape = stringToShape(shapeAsString)
    spatialSearch(shape, k, operationName)(indexSearcher)
  }

  def spatialSearch(point: PointType, k: Int, operationName: String)
                   (indexSearcher: IndexSearcher)
  : Array[SparkScoreDoc] = {
    val shape = ctx.makePoint(point._1, point._2)
    spatialSearch(shape, k, operationName)(indexSearcher)
  }

  def bboxSearch(center: PointType, radius: Double, k: Int, operationName: String)
                (indexSearcher: IndexSearcher)
  : Array[SparkScoreDoc] = {
    val x = center._1
    val y = center._2
    val radiusKM = DistanceUtils.dist2Degrees(radius, DistanceUtils.EARTH_MEAN_RADIUS_KM)
    val shape = ctx.makeRectangle(x - radiusKM, x + radiusKM, y - radiusKM, y + radiusKM)
    spatialSearch(shape, k, operationName)(indexSearcher)
  }

  def bboxSearch(lowerLeft: PointType, upperRight: PointType, k: Int, opName: String)
                (indexSearcher: IndexSearcher)
  : Array[SparkScoreDoc] = {
    val lowerLeftPt = ctx.makePoint(lowerLeft._1, lowerLeft._2)
    val upperRightPt = ctx.makePoint(upperRight._1, upperRight._2)
    val shape = ctx.makeRectangle(lowerLeftPt, upperRightPt)
    spatialSearch(shape, k, opName)(indexSearcher)
  }
}
