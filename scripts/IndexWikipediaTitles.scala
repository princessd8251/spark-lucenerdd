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

import scala.io.Source
import org.zouzias.spark.lucenerdd._
import org.zouzias.spark.lucenerdd.LuceneRDD
val wiki= spark.read.parquet("recordLinkageData/wikipedia/enwiki-latest-all-titles.parquet")

sc.setLogLevel("DEBUG")

val start = System.currentTimeMillis()
val luceneRDD = LuceneRDD(wiki.sample(true, 0.04))
luceneRDD.cache
luceneRDD.count
val end = System.currentTimeMillis()

luceneRDD.close

println(s"Indexing time: ${end - start}")