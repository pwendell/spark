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

package org.apache.spark.network

import java.net.{InetAddress, InetSocketAddress}

import scala.collection.mutable

import org.apache.spark.util.Utils

private[spark] case class ConnectionManagerId(host: String, port: Int) {
  // DEBUG code
  Utils.checkHost(host)
  assert (port > 0)

  def toSocketAddress() = new InetSocketAddress(host, port)
}


private[spark] object ConnectionManagerId {
  /** We cache host resolution here to avoid frequent calls into the OS networking stack. */
  private val hostCache = mutable.HashMap[InetAddress, String]()

  def fromSocketAddress(socketAddress: InetSocketAddress): ConnectionManagerId = {
    val address = socketAddress.getAddress
    if (!hostCache.contains(address)) {
      hostCache.put(address, address.getHostName)
    }
    val hostName = hostCache.get(address).get
    new ConnectionManagerId(hostName, socketAddress.getPort())
  }
}
