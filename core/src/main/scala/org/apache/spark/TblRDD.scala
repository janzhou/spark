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

package org.apache.spark

/**
 * Created by jan on 2/19/15.
 */
class TblRDD (sc: SparkContext, dir: String) {
  val customer = sc.textFile(dir + "/customer.tbl").map { line =>
    val item = line.split("\\|")
    Map(
      "CUSTKEY" -> item(0),
      "NAME" -> item(1),
      "ADDRESS" -> item(2),
      "NATIONKEY" -> item(3),
      "PHONE" -> item(4),
      "ACCTBAL" -> item(5),
      "MKTSEGMENT" -> item(6),
      "COMMENT" -> item(7)
    )
  }

  val lineitem = sc.textFile(dir + "/lineitem.tbl").map { line =>
    val item = line.split("\\|")
    Map(
      "ORDERKEY" -> item(0),
      "PARTKEY" -> item(1),
      "SUPPKEY" -> item(2),
      "LINENUMBER" -> item(3),
      "QUANTITY" -> item(4),
      "EXTENDEDPRICE" -> item(5),
      "DISCOUNT" -> item(6),
      "TAX" -> item(7),
      "RETURNFLAG" -> item(8),
      "LINESTATUS" -> item(9),
      "SHIPDATE" -> item(10),
      "COMMITDATE" -> item(11),
      "RECEIPTDATE" -> item(12),
      "SHIPINSTRUCT" -> item(13),
      "SHIPMODE" -> item(14),
      "COMMENT" -> item(15)
    )
  }

  val nation   = sc.textFile(dir + "/nation.tbl").map { line =>
    val item = line.split("\\|")
    Map(
      "NATIONKEY" -> item(0),
      "NAME" -> item(1),
      "REGIONKEY" -> item(2),
      "COMMENT" -> item(3)
    )
  }

  val orders   = sc.textFile(dir + "/orders.tbl").map { line =>
    val item = line.split("\\|")
    Map(
      "ORDERKEY" -> item(0),
      "CUSTKEY" -> item(1),
      "ORDERSTATUS" -> item(2),
      "TOTALPRICE" -> item(3),
      "ORDERDATE" -> item(4),
      "ORDERPRIORITY" -> item(5),
      "CLERK" -> item(6),
      "SHIPPRIORITY" -> item(7),
      "COMMENT" -> item(8)
    )
  }

  val part     = sc.textFile(dir + "/part.tbl").map { line =>
    val item = line.split("\\|")
    Map(
      "PARTKEY" -> item(0),
      "NAME" -> item(1),
      "MFGR" -> item(2),
      "BRAND" -> item(3),
      "TYPE" -> item(4),
      "SIZE" -> item(5),
      "CONTAINER" -> item(6),
      "RETAILPRICE" -> item(7),
      "COMMENT" -> item(8)
    )
  }

  val partsupp = sc.textFile(dir + "/partsupp.tbl").map { line =>
    val item = line.split("\\|")
    Map(
      "PARTKEY" -> item(0),
      "SUPPKEY" -> item(1),
      "AVAILQTY" -> item(2),
      "SUPPLYCOST" -> item(3),
      "COMMENT" -> item(4)
    )
  }

  val region   = sc.textFile(dir + "/region.tbl").map { line =>
    val item = line.split("\\|")
    Map(
      "REGIONKEY" -> item(0),
      "NAME" -> item(1),
      "COMMENT" -> item(2)
    )
  }

  val supplier = sc.textFile(dir + "/supplier.tbl").map { line =>
    val item = line.split("\\|")
    Map(
      "SUPPKEY" -> item(0),
      "NAME" -> item(1),
      "ADDRESS" -> item(2),
      "NATIONKEY" -> item(3),
      "PHONE" -> item(4),
      "ACCTBAL" -> item(5),
      "COMMENT" -> item(6)
    )
  }
}
