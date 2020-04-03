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

package org.apache.spark.sql.execution.mv

import scala.collection.mutable

class EquivalenceClasses {

  var nodeToEquivalenceClass: mutable.Map[String, mutable.Set[String]]
  = mutable.Map.empty
  var cacheEquivalenceClassesMap: mutable.Map[String, mutable.Set[String]]
  = mutable.Map.empty
  var cacheEquivalenceClasses: mutable.Seq[mutable.Set[String]] = mutable.Seq.empty

  def addEquivalenceClass(attRef1: String, attRef2: String): Unit = {
    // Clear cache
    cacheEquivalenceClassesMap.clear
    cacheEquivalenceClasses = mutable.Seq.empty

    var c1 = nodeToEquivalenceClass.get(attRef1).getOrElse(mutable.Set.empty)
    var c2 = nodeToEquivalenceClass.get(attRef2).getOrElse(mutable.Set.empty)
    if (!c1.isEmpty && !c2.isEmpty) {
      // Both present, we need to merge
      c1 ++= c2
      for (attRef <- c2) {
        nodeToEquivalenceClass(attRef) = c1
      }
    } else if (!c1.isEmpty) {
      // attRef1 present, we need to merge into it
      c1 += attRef2
      nodeToEquivalenceClass(attRef2) = c1
    } else if (!c2.isEmpty) {
      // attRef2 present, we need to merge into it
      c2 += attRef1
      nodeToEquivalenceClass(attRef1) = c2
    } else {
      // None are present, add to same equivalence class
      val equivalenceClass = mutable.HashSet[String](attRef1, attRef2)
      nodeToEquivalenceClass(attRef1) = equivalenceClass
      nodeToEquivalenceClass(attRef2) = equivalenceClass
    }
  }

  def getEquivalenceClassesMap: mutable.Map[String, mutable.Set[String]] = {
    if (cacheEquivalenceClassesMap.isEmpty) {
      cacheEquivalenceClassesMap = nodeToEquivalenceClass.clone()
    }
    cacheEquivalenceClassesMap
  }

  def getEquivalenceClasses: mutable.Seq[mutable.Set[String]] = {
    if (cacheEquivalenceClasses.isEmpty) {
      val visited: mutable.Set[String] = mutable.Set.empty
      for (equalClasses <- nodeToEquivalenceClass.values) {
        if (!visited.exists(equalClasses.contains(_))) {
          visited ++= equalClasses
          cacheEquivalenceClasses :+= equalClasses
        }
      }
    }
    cacheEquivalenceClasses
  }

  def copy: EquivalenceClasses = {
    val newEc: EquivalenceClasses = new EquivalenceClasses
    for ((key, value) <- nodeToEquivalenceClass) {
      newEc.nodeToEquivalenceClass(key) -> value.clone()
    }
    newEc.cacheEquivalenceClassesMap = mutable.Map.empty
    newEc.cacheEquivalenceClasses = mutable.Seq.empty
    newEc
  }

  def contains(other: EquivalenceClasses): Boolean = {
    val otherMap = other.getEquivalenceClassesMap
    getEquivalenceClassesMap.size >= otherMap.size &&
      otherMap.keySet.filter(k => !getEquivalenceClassesMap.keySet.contains(k)).isEmpty
  }
}
