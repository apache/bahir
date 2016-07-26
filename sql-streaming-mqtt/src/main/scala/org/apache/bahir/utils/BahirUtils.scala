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

package org.apache.bahir.utils

import java.io.{File, IOException}
import java.nio.file.{Files, FileVisitResult, Path, SimpleFileVisitor}
import java.nio.file.attribute.BasicFileAttributes

object BahirUtils extends Logging {

  def recursiveDeleteDir(dir: File): Path = {
    Files.walkFileTree(dir.toPath, new SimpleFileVisitor[Path]() {
      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        try {
          Files.delete(file)
        } catch {
          case t: Throwable => log.warn("Failed to delete", t)
        }
        FileVisitResult.CONTINUE
      }

      override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
        try {
          Files.delete(dir)
        } catch {
          case t: Throwable => log.warn("Failed to delete", t)
        }
        FileVisitResult.CONTINUE
      }
    })
  }

}
