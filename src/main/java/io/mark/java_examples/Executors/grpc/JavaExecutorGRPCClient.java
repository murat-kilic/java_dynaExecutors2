package io.mark.java_examples.Executors.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import com.google.protobuf.ByteString;
import io.mark.java_examples.Executables.AddPersonResponse;

import java.io.BufferedInputStream;
import java.io.FileInputStream;

import java.io.ObjectInputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class JavaExecutorGRPCClient
{

    private final ManagedChannel channel;
    private final FunctionServiceGrpc.FunctionServiceBlockingStub blockingStub;
    private final FunctionServiceGrpc.FunctionServiceStub asyncStub;

    private  JavaExecutorGRPCClient(String host, int port) {
        channel=ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        blockingStub = FunctionServiceGrpc.newBlockingStub(channel);
        asyncStub = FunctionServiceGrpc.newStub(channel);

    }

    private void uploadFile(String functionName,String fileName) {
        System.out.println("Starting uploadFile function name:"+functionName+" file name:"+fileName);

        final CountDownLatch finishLatch = new CountDownLatch(1);

        StreamObserver<UploadFileResponse> responseObserver = new StreamObserver<UploadFileResponse>() {
            public void onNext(UploadFileResponse res) {
                System.out.println("Uploaded: " + res.getUploaded());
            }

            public void onError(Throwable t) {
                System.out.println("Upload File Failed: {0} \n" + Status.fromThrowable(t));
                finishLatch.countDown();
            }

            public void onCompleted() {
                System.out.println("Finished uploadFile");
                finishLatch.countDown();
            }
        };

        try{
            StreamObserver<UploadFileRequest> requestObserver = asyncStub.uploadFile(responseObserver);
            requestObserver.onNext(
                    UploadFileRequest.newBuilder().setFunctionName(functionName).setJarFile(fileName).build()
            );

            FileInputStream fis = new FileInputStream(System.getProperty("java.io.tmpdir")+fileName);
            BufferedInputStream bis = new BufferedInputStream(fis);
            int bufferSize = 256 * 1024;
            byte[] buffer = new byte[bufferSize];
            int length;
            while ((length = bis.read(buffer, 0, bufferSize)) != -1) {
                requestObserver.onNext(
                        UploadFileRequest.newBuilder().setFileContent(ByteString.copyFrom(buffer, 0, length)).build()
                );

            }
            requestObserver.onCompleted();
            finishLatch.await(1, TimeUnit.MINUTES);
        }catch(Exception e){e.printStackTrace();}

    }

    private void addFunction(String functionName,String handler) {
        System.out.println("Starting addFx");
        AddRequest req = AddRequest.newBuilder().setName(functionName).setHandler(handler).build();

        try {
            AddResponse res = blockingStub.add(req);
            System.out.println("Added:"+res);
        } catch (StatusRuntimeException e) {
            System.out.println("RPC failed: {0} "+e.getStatus());
        }

    }

    private void updateFunction(String fxName,String handler) {
        System.out.println("Starting updateFunction function name:"+fxName+" handler:"+handler);
        UpdateRequest req = UpdateRequest.newBuilder().setName(fxName).setHandler(handler).build();
         try {
            UpdateResponse res = blockingStub.update(req);
            System.out.println("Updated:"+res);
        } catch (StatusRuntimeException e) {
            System.out.println("RPC failed: {0} "+e.getStatus());
        }

    }

    private void listFunctions() {
        System.out.println("Starting listFunctions");
        ListRequest req = ListRequest.newBuilder().build();

        try {
            ListResponse res = blockingStub.list(req);
            System.out.println("List of Functions \n");
            res.getFunctionList().forEach(function -> {
                System.out.println("Function:"+function.getName()+" handler:"+function.getHandler()+" JAR File:"+function.getJarFile());
        });
        } catch (StatusRuntimeException e) {
            System.out.println("RPC failed: {0} "+e.getStatus());
        }

    }

    private void getFunction(String functionName) {
        System.out.println("Starting getFunction name:"+functionName);
        GetRequest req = GetRequest.newBuilder().setName(functionName).build();

        try {
            GetResponse res = blockingStub.get(req);
            System.out.println("Function name:"+functionName+" Handler:"+res.getHandler()+" Jar File:"+res.getJarFile()+"\n");
        } catch (StatusRuntimeException e) {
            System.out.println("RPC failed: {0} "+e.getStatus());
        }

    }
    private Object executeFunction(String functionName,String data) {
        return executeFunction(functionName, data, null);
    }

    private Object executeFunction(String functionName,String data,String requestedOutputFormat) {
        try {

            //ExecutionRequest request = ExecutionRequest.newBuilder().setName(functionName).setInput(data).build();
            ExecutionRequest.Builder reqBuilder = ExecutionRequest.newBuilder().setName(functionName).setInput(data);

            if (requestedOutputFormat != null) {
                reqBuilder.setSerializationFormat(requestedOutputFormat);
            }

            ExecutionResponse response = blockingStub.execute(reqBuilder.build());
            if (response.getOutputOneofCase().equals(ExecutionResponse.OutputOneofCase.JSONOUT)) {
                return(response.getJsonOut());
            }else {
                ObjectInputStream in = new ObjectInputStream(response.getJavaOut().newInput());
                AddPersonResponse person = (AddPersonResponse) in.readObject();
                in.close();
                return person;
                  /* In case we want to convert to JSON for any other reason we can use this code
            Gson gson = new GsonBuilder()
                    .setLenient()
                    .create();
            String jsonOut=gson.toJson(person)
            */
            }
        } catch (StatusRuntimeException e) {
            System.out.println("RPC failed: {0} "+e.getStatus());
            return null;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }


private void deleteFunction(String functionName) {
        System.out.println("Starting deleteFunction name:"+functionName);
        DeleteRequest req = DeleteRequest.newBuilder().setName(functionName).build();

        try {
            DeleteResponse res = blockingStub.delete(req);
            if (res.getDeleted())
                System.out.println("Deleted "+functionName);
            else
                System.out.println("Could not delete "+functionName);
        } catch (StatusRuntimeException e) {
            System.out.println("RPC failed: {0} "+e.getStatus());
        }

    }
	    public static void main( String[] args ) {
            JavaExecutorGRPCClient client = new JavaExecutorGRPCClient("localhost", 8080);
            Object returnedObj;
            /*client.addFunction("MyFunction","io.mark.java_examples.Executables.RunThisCode:handleRequest");
            client.addFunction("MyFunction2","io.mark.java_examples.Executables.RunThisCode:run");
            client.addFunction("MyFunction3","io.mark.java_examples.Executables.RunThisCode:handleRequestStr");
            client.addFunction("MyFunction4","io.mark.java_examples.Executables.RunThisCode:handleRequestInt");
            client.uploadFile("MyFunction","ExecutableCode-1.0-SNAPSHOT.jar");
            client.uploadFile("MyFunction2","ExecutableCode-1.0-SNAPSHOT.jar");
            client.deleteFunction("MyFunction3");
            client.listFunctions();
            client.getFunction("MyFunction");
            client.executeFunction("MyFunction","");
            client.updateFunction("MyFunction2","io.mark.java_examples.Executables.RunThisCode:addPerson");
            client.listFunctions();
           */
            //client.addFunction("MyFunction2","io.mark.java_examples.Executables.RunThisCode::addPerson");
            //client.uploadFile("MyFunction2","ExecutableCode-1.0-SNAPSHOT.jar");
//client.updateFunction("MyFunction2","io.mark.java_examples.Executables.RunThisCode::addPerson");



            // Let's execute AddPerson without specifying output format which should return a JSON string
            returnedObj=client.executeFunction("MyFunction2","{\"firstName\":\"Mark\",\"lastName\":\"Kose\",\"age\":30,\"isActive\":false}");
            System.out.println("Execution returned:"+returnedObj);

             // Let's execute AddPerson but request a Java object returned
            returnedObj=client.executeFunction("MyFunction2","{\"firstName\":\"Mark\",\"lastName\":\"Kose\",\"age\":30,\"isActive\":false}","java");
            AddPersonResponse person=(AddPersonResponse)returnedObj;
            System.out.print("Added " + person.fullName + " with age: " + person.age);
            if (person.shouldExerciseMore)
                System.out.print(" but I gotta say, this person should exercise more");
            else
                System.out.print(" and this person seems to be exercising enough");


            client.channel.shutdownNow();

        }

}
