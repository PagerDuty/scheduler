package com.pagerduty.scheduler

import com.pagerduty.eris.ClusterCtx
import com.pagerduty.scheduler.model.Task
import com.pagerduty.scheduler.specutil.{TaskFactory, UnitSpec}
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers
import org.scalatest.concurrent.{ScalaFutures, Eventually}
import scala.concurrent.Await

class CassandraTaskExecutorServiceSpec
    extends UnitSpec
    with Matchers
    with Eventually
    with MockFactory
    with ScalaFutures {
  val partitionId = 1

  val numberOfThreads = 2
  trait Resource
  class MockClusterCtx extends ClusterCtx("testCluster", null, null, null)

  "A CassandraTaskExecutorService should" should {
    "setup managed resource" in {
      val taskRunner = mock[ManagedCassandraTaskRunner[Resource]]
      val clusterCtx = mock[MockClusterCtx]

      (taskRunner.makeClusterCtx _).expects().returns(clusterCtx)
      (clusterCtx.start _).expects()
      (taskRunner.makeManagedResource _).expects(clusterCtx)

      new CassandraTaskExecutorService(numberOfThreads, taskRunner)
    }

    "shutdown cassandra cluster connection pool" in {
      val taskRunner = mock[ManagedCassandraTaskRunner[Resource]]
      val clusterCtx = mock[MockClusterCtx]

      (taskRunner.makeClusterCtx _).stubs().returns(clusterCtx)
      (clusterCtx.start _).stubs()
      (taskRunner.makeManagedResource _).stubs(*)
      val service = new CassandraTaskExecutorService(numberOfThreads, taskRunner)

      (clusterCtx.shutdown _).expects()
      service.shutdown()
    }

    "run task with managed resource" in {
      val clusterCtx = mock[MockClusterCtx]
      (clusterCtx.start _).stubs()

      val mockResource = new Resource {}

      @volatile var executedTask: Task = null
      val taskRunner = new ManagedCassandraTaskRunner[Resource] {
        def makeClusterCtx(): ClusterCtx = clusterCtx
        def makeManagedResource(clusterCtx: ClusterCtx): Resource = mockResource
        def runTask(task: Task, resource: Resource): Unit = {
          assert(resource == mockResource)
          executedTask = task
        }
      }
      val service = new CassandraTaskExecutorService(numberOfThreads, taskRunner)

      val task = TaskFactory.makeTask()
      service.execute(partitionId, task)

      eventually {
        executedTask shouldEqual task
      }
    }

    "correctly handle failing task" in {
      val clusterCtx = mock[MockClusterCtx]
      (clusterCtx.start _).stubs()
      val mockResource = new Resource {}

      val taskRunner = new ManagedCassandraTaskRunner[Resource] {
        def makeClusterCtx(): ClusterCtx = clusterCtx
        def makeManagedResource(clusterCtx: ClusterCtx): Resource = mockResource
        def runTask(task: Task, resource: Resource): Unit = {
          throw new RuntimeException("Simulated test exception.")
        }
      }
      val service = new CassandraTaskExecutorService(numberOfThreads, taskRunner)

      val task = TaskFactory.makeTask()
      val futureResult = service.execute(partitionId, task)

      val thrown = the[RuntimeException] thrownBy { futureResult.futureValue }
      thrown.getMessage should include("Simulated test exception.")
    }
  }
}
