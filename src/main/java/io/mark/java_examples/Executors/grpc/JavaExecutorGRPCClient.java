package io.mark.java_examples.Executors.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import com.google.protobuf.ByteString;
import io.mark.java_examples.Executables.AddPersonResponse;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;

import java.io.ObjectInputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JavaExecutorGRPCClient
{
    final static Logger logger = LoggerFactory.getLogger(JavaExecutorGRPCClient.class);
    public final ManagedChannel channel;
    public final FunctionServiceGrpc.FunctionServiceBlockingStub blockingStub;
    public final FunctionServiceGrpc.FunctionServiceStub asyncStub;

    public  JavaExecutorGRPCClient(String host, int port) {
        channel=ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        blockingStub = FunctionServiceGrpc.newBlockingStub(channel);
        asyncStub = FunctionServiceGrpc.newStub(channel);

    }

    public boolean uploadFile(String functionName,String fileName) {
        logger.info("Starting uploadFile function name:"+functionName+" file name:"+fileName);

        final CountDownLatch finishLatch = new CountDownLatch(1);

        StreamObserver<UploadFileResponse> responseObserver = new StreamObserver<UploadFileResponse>() {
            public void onNext(UploadFileResponse res) {
             }

            public void onError(Throwable t) {
                logger.error("Upload File Failed: {0} \n" + Status.fromThrowable(t));
                finishLatch.countDown();
            }

            public void onCompleted() {
                logger.debug("Finished uploadFile");
                finishLatch.countDown();
            }
        };

        try{
            StreamObserver<UploadFileRequest> requestObserver = asyncStub.uploadFile(responseObserver);
            requestObserver.onNext(
                    UploadFileRequest.newBuilder().setFunctionName(functionName).setJarFile(fileName).build()
            );

            FileInputStream fis = new FileInputStream(System.getProperty("java.io.tmpdir")+ File.separator+fileName);
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
            return true;
        }catch(Exception e){
            logger.error("Exception occured", e);
            return false;
            }

    }

    public boolean addFunction(String functionName,String handler) {
        logger.info("Starting addFunction function name:"+functionName+" handler:"+handler);
          AddRequest req = AddRequest.newBuilder().setName(functionName).setHandler(handler).build();
     try {
            AddResponse res = blockingStub.add(req);
            boolean added=res.getAdded();
            logger.debug("Added:"+added);
            return added;
        } catch (StatusRuntimeException e) {
            logger.error("RPC failed: {0} "+e.getStatus());
            return false;
        }

    }

    public boolean updateFunction(String functionName,String handler) {
        logger.info("Starting updateFunction function name:"+functionName+" handler:"+handler);
        UpdateRequest req = UpdateRequest.newBuilder().setName(functionName).setHandler(handler).build();
         try {
            UpdateResponse res = blockingStub.update(req);
            return res.getUpdated();
        } catch (StatusRuntimeException e) {
             logger.error("RPC failed: {0} "+e.getStatus());
             return false;
        }
    }

    public java.util.List<io.mark.java_examples.Executors.grpc.Function> listFunctions() {
        logger.info("Starting listFunctions");
        ListRequest req = ListRequest.newBuilder().build();
        try {
            ListResponse res = blockingStub.list(req);
             return res.getFunctionList();
        } catch (StatusRuntimeException e) {
            logger.error("RPC failed: {0} "+e.getStatus());
            return null;
        }

    }

    public GetResponse getFunction(String functionName) {
        logger.info("Starting getFunction name:"+functionName);
        GetRequest req = GetRequest.newBuilder().setName(functionName).build();

        try {
            GetResponse res = blockingStub.get(req);
            return res;
            //System.out.println("Function name:"+functionName+" Handler:"+res.getHandler()+" Jar File:"+res.getJarFile()+"\n");
        } catch (StatusRuntimeException e) {
            logger.error("RPC failed: {0} ",e);
            return null;
        }
    }

    public Object executeFunction(String functionName,String data) {
        return executeFunction(functionName, data, null);
    }

    public Object executeFunction(String functionName,String data,String requestedOutputFormat) {
        logger.info("Starting executeFunction name:"+functionName);
        try {
              ExecutionRequest.Builder reqBuilder = ExecutionRequest.newBuilder().setName(functionName).setInput(data);

            if (requestedOutputFormat != null) {
                reqBuilder.setSerializationFormat(requestedOutputFormat);
            }

            ExecutionResponse response = blockingStub.execute(reqBuilder.build());
            if (response.getOutputOneofCase().equals(ExecutionResponse.OutputOneofCase.JSONOUT)) {
                logger.debug("Output is JSON");
                return(response.getJsonOut());
            }else {
                logger.debug("Output is Java");
                ObjectInputStream in = new ObjectInputStream(response.getJavaOut().newInput());
                Object obj=in.readObject();
                in.close();
                return obj;

            }
        } catch (StatusRuntimeException e) {
            logger.error("RPC failed: {0} ",e);
            return null;
        } catch (Exception e) {
            logger.error("Exception occured: ",e);
            return null;
        }
    }


    public boolean deleteFunction(String functionName) {
        logger.info("Starting deleteFunction name:"+functionName);
        DeleteRequest req = DeleteRequest.newBuilder().setName(functionName).build();

        try {
            DeleteResponse res = blockingStub.delete(req);
            return res.getDeleted();
        } catch (StatusRuntimeException e) {
            logger.error("RPC failed: {0} ",e);
            return false;
        }

    }
	    public static void main( String[] args ) {
            JavaExecutorGRPCClient client = new JavaExecutorGRPCClient("localhost", 8080);
            Object returnedObj;
            client.addFunction("MyFunction","io.mark.java_examples.Executables.RunThisCode::handleRequest");
            client.addFunction("MyFunction2","io.mark.java_examples.Executables.RunThisCode::run");
            client.addFunction("MyFunction3","io.mark.java_examples.Executables.RunThisCode::handleRequestStr");
            client.uploadFile("MyFunction","ExecutableCode-1.0-SNAPSHOT.jar");
            client.uploadFile("MyFunction2","ExecutableCode-1.0-SNAPSHOT.jar");
            client.deleteFunction("MyFunction3");
            client.listFunctions();
            client.getFunction("MyFunction");
            System.out.println("MyFunction Execution result:"+client.executeFunction("MyFunction",""));
            client.updateFunction("MyFunction2","io.mark.java_examples.Executables.RunThisCode::addPerson");
            client.listFunctions();
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
