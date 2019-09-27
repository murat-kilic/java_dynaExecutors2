package io.mark.java_examples.Executors.grpc;

import io.mark.java_examples.Executables.AddPersonResponse;
import org.junit.*;

import static org.junit.jupiter.api.Assertions.*;

public class TestJavaExecutionGRPCService {

    static JavaExecutorGRPCClient client;
    @BeforeClass
    public static  void setup() throws Exception {
        System.out.println("Setting up test environment");
        JavaExecutorGRPCServer jes=new JavaExecutorGRPCServer();
        jes.startServer();
         client = new JavaExecutorGRPCClient("localhost", 8080);
         }


    @AfterClass //
    public static void tearDown() throws Exception {
        client.channel.shutdownNow();
    }

    @Test
    public void testAddFunction() {
      assertTrue(client.addFunction("AddMyFunction", "io.mark.java_examples.Executables.RunThisCode::handleRequest"));
     assertTrue(client.addFunction("AddMyFunction2","io.mark.java_examples.Executables.RunThisCode::run"));
     assertTrue(!client.addFunction("AddMyFunction", "io.mark.java_examples.Executables.RunThisCode::handleRequest"));
    }

    @Test
    public void testGetFunction() {
        assertTrue(client.addFunction("GetMyFunction", "io.mark.java_examples.Executables.RunThisCode::handleRequest"));
        assertEquals(client.getFunction("GetMyFunction"),GetResponse.newBuilder().setHandler("io.mark.java_examples.Executables.RunThisCode::handleRequest").build());
    }

    @Test
    public void testDeleteFunction() {
        assertTrue(client.addFunction("MyFunctionToBeDeleted", "io.mark.java_examples.Executables.RunThisCode::handleRequest"));
        assertTrue(client.deleteFunction("MyFunctionToBeDeleted"));
    }

    @Test
    public void testUpdateFunction() {
        assertTrue(client.addFunction("UpdateMyFunction", "io.mark.java_examples.Executables.RunThisCode::handleRequest"));
        assertTrue(client.addFunction("UpdateMyFunction2","io.mark.java_examples.Executables.RunThisCode::run"));
        assertTrue(client.updateFunction("UpdateMyFunction","io.mark.java_examples.Executables.RunThisCode::addPerson"));
        assertEquals(client.getFunction("UpdateMyFunction"),GetResponse.newBuilder().setHandler("io.mark.java_examples.Executables.RunThisCode::addPerson").build());
    }

    @Test
    public void testListFunctions() {
        assertTrue(client.addFunction("ListMyFunction", "io.mark.java_examples.Executables.RunThisCode::handleRequest"));
        assertTrue(client.addFunction("ListMyFunction2", "io.mark.java_examples.Executables.RunThisCode::run"));
        assertTrue(client.addFunction("ListMyFunction3", "io.mark.java_examples.Executables.RunThisCode::run"));
        assertNotNull(client.listFunctions());
    }

    @Test
    public void testUploadFile() {
        assertTrue(client.addFunction("MyFunctionforUpload", "io.mark.java_examples.Executables.RunThisCode:handleRequest"));
        assertTrue(client.uploadFile("MyFunctionforUpload", "ExecutableCode-1.0-SNAPSHOT.jar"));
    }

    @Test
    public void testExecuteFunction() {
        assertTrue(client.addFunction("ExecuteMyFunction", "io.mark.java_examples.Executables.RunThisCode::addPerson"));
        assertTrue(client.uploadFile("ExecuteMyFunction", "ExecutableCode-1.0-SNAPSHOT.jar"));
        assertEquals(client.executeFunction("ExecuteMyFunction", "{\"firstName\":\"Mark\",\"lastName\":\"Kose\",\"age\":30,\"isActive\":false}"),"{\"fullName\":\"Mark Kose\",\"age\":30,\"shouldExerciseMore\":true}");
        AddPersonResponse person=(AddPersonResponse)client.executeFunction("ExecuteMyFunction","{\"firstName\":\"Mark\",\"lastName\":\"Kose\",\"age\":30,\"isActive\":false}","java");
        assertEquals(person.fullName,"Mark Kose");
        assertTrue(person.shouldExerciseMore);
    }

}
