/*
 * Copyright 2009-2010 LinkedIn, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.linkedin.norbert
package network
package common

import org.specs.Specification
import org.specs.mock.Mockito
import java.util.concurrent.{TimeoutException, ExecutionException, TimeUnit, Executor, LinkedBlockingQueue, Executors}
import scala.Right

class CurrentThreadExecutor extends Executor {
    def execute(r:Runnable) = {
        r.run();
    }
}

class NorbertFutureSpec extends Specification with Mockito with SampleMessage {
  case class ResponseExceptionWrapper(exception:ExecutionException, ping: Ping, isException:Boolean)
  //val future = new FutureAdapter[Ping]
  val future = new FutureAdapterListener[Ping]
  val queue = new LinkedBlockingQueue[ResponseExceptionWrapper]()
  future.addListener(new Runnable() {
    def run = {
      try {
        val result = future.get
        queue.offer(ResponseExceptionWrapper(null, result, false))
      } catch {
        case e:ExecutionException => queue.offer(ResponseExceptionWrapper(e, null, true))
      }
    }
  }, Executors.newFixedThreadPool(1)
  )

  "NorbertFuture" should {
    "not be done when created" in {
      future.isDone must beFalse
    }

    "be done when value is set" in {
      future.apply(Right(new Ping))
      future.isDone must beTrue
      queue.size must be(1)
    }

    "be done when value is set" in {
      val future = new FutureAdapterListener[Ping]
      val queue = new LinkedBlockingQueue[ResponseExceptionWrapper]()

      val msg = new Ping
      future.apply(Right(msg))
      future.isDone must beTrue
      queue.size must be(0)
      future.addListener(new Runnable() {
        def run = {
          try {
            val result = future.get
            queue.offer(ResponseExceptionWrapper(null, result, false))
          } catch {
          case e:ExecutionException => queue.offer(ResponseExceptionWrapper(e, null, true))
          }
        }
      }, Executors.newFixedThreadPool(1))
      queue.size must be(1)
      queue.poll mustEqual ResponseExceptionWrapper(null, msg, false)
    }

    "return the value that is set" in {
      val message = new Ping
      future.apply(Right(request))
      future.get must be(request)
      future.get(1, TimeUnit.MILLISECONDS) must be(request)
    }

    "throw a TimeoutException if no response is available" in {
      future.get(1, TimeUnit.MILLISECONDS) must throwA[TimeoutException]
    }

    "throw an ExecutionExcetion for an error" in {
      val ex = new Exception
      future.apply(Left(ex))
      future.get must throwA[ExecutionException]
      future.get(1, TimeUnit.MILLISECONDS) must throwA[ExecutionException]
      queue.size must be(1)
      val response: ResponseExceptionWrapper = queue.poll
      response.isException mustEqual true
    }
  }
}
