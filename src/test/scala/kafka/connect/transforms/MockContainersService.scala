package kafka.connect.transforms

import com.dimafeng.testcontainers.lifecycle.and
import com.dimafeng.testcontainers.{DockerComposeContainer, ExposedService, GenericContainer, MySQLContainer, PostgreSQLContainer}
import com.dimafeng.testcontainers.scalatest.TestContainersForAll
import org.junit.Test
import org.scalatest.flatspec.AnyFlatSpec
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy

import java.io.File

@Test
class MockContainersService extends AnyFlatSpec with TestContainersForAll{
  // First of all, you need to declare, which containers you want to use
  override type Containers =  DockerComposeContainer

  // After that, you need to describe, how you want to start them,
  // In this method you can use any intermediate logic.
  // You can pass parameters between containers, for example.
  override def startContainers(): Containers = {
    //val container1 = MySQLContainer.Def().start()
    //val container2 = PostgreSQLContainer.Def().start()
    val container3 =
      DockerComposeContainer
        .Def(DockerComposeContainer.ComposeFile(Left(new File("src/test/resources/docker-compose.yml"))),
          exposedServices = Seq(ExposedService("postgres-server", port = 5432, new LogMessageWaitStrategy().withRegEx(".*Ready\\.\n")) )
        )
        .start()
      container3
  }

  // `withContainers` function supports multiple containers:
  it should "test" in withContainers { case  dcContainer =>
    // Inside your test body you can do with your containers whatever you want to
    assert(true )
  }

}