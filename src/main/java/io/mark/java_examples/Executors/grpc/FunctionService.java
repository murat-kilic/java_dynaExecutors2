package io.mark.java_examples.Executors.grpc;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import com.google.common.io.ByteSink;
import com.google.common.io.FileWriteMode;
import com.google.common.io.Files;
import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

public class FunctionService extends  FunctionServiceGrpc.FunctionServiceImplBase {

	// Contains all functions defined
	HashMap<String,Function> functions =new HashMap<String,Function>();

	@Override
	public StreamObserver<UploadFileRequest> uploadFile(final StreamObserver<UploadFileResponse> responseObserver) {

	return new StreamObserver<UploadFileRequest>() {
		boolean fxNameSet=false;
		String fxName=null,jarFile=null;
		ByteSink byteSink=null;
		File uploadedFile=null;
			@Override
			public void onNext(UploadFileRequest req) {
				try{
				if (!fxNameSet) {
					if (req.getFunctionName()!=null) {
						fxName=req.getFunctionName();
						jarFile=req.getJarFile();
						System.out.println("Received Function Name:"+fxName+" JAR File:"+req.getJarFile()+"\n");
						if (!req.getJarFile().endsWith(".jar") && !req.getJarFile().endsWith(".JAR")) {
						 responseObserver.onError(Status.FAILED_PRECONDITION.augmentDescription("You can only upload JAR files").asRuntimeException());
							return;
						}
						// Let's create uploaded directory if it does not exist
						Path uploadDir=Paths.get(System.getProperty("java.io.tmpdir")+"uploaded"+File.separator+req.getFunctionName());
						if (!java.nio.file.Files.exists(uploadDir))
							java.nio.file.Files.createDirectory(uploadDir);

						uploadedFile = new File(uploadDir+File.separator+req.getJarFile());
						byteSink = Files.asByteSink(uploadedFile, FileWriteMode.APPEND);
						fxNameSet=true;
					}
				}else {
					System.out.println("Received data:" + req.getFileContent() + "\n");
					byteSink.write(req.getFileContent().toByteArray());
				}
				}catch (IOException e) {
					e.printStackTrace();
					responseObserver.onError(e);
				}
			}
			@Override
			public void onError(Throwable t) {
				System.out.println("uploadFile cancelled");
			}
			@Override
			public void onCompleted() {
				Function fx=functions.get(fxName);
				if (fx != null) {
					functions.put(fxName, Function.newBuilder().setName(fxName).setHandler(fx.getHandler()).setJarFile(jarFile).build());
					responseObserver.onNext(UploadFileResponse.newBuilder().setUploaded(true).build());
					responseObserver.onCompleted();
					System.out.println("uploadFile completed");
				}else {
					responseObserver.onError(Status.NOT_FOUND.augmentDescription("Function does not exist").asRuntimeException());
				}
			}
		};

	}

		@Override
	public void add(io.mark.java_examples.Executors.grpc.AddRequest request,
					io.grpc.stub.StreamObserver<io.mark.java_examples.Executors.grpc.AddResponse> responseObserver) {
		// Check if it exists first
			String fxName=request.getName();
			if (functions.get(fxName) != null) {
				responseObserver.onError(Status.ALREADY_EXISTS.augmentDescription("Function exists").asRuntimeException());
			return;
			}

		System.out.println("Adding function:"+request.getName());
		Function fn=Function.newBuilder().setName(request.getName()).setHandler(request.getHandler()).build();
		functions.put(fxName,fn);

		//AddResponse.Builder resBuilder = io.mark.java_examples.Executors.grpc.AddResponse.newBuilder();
		responseObserver.onNext(AddResponse.newBuilder().setAdded(true).build());
		responseObserver.onCompleted();

			System.out.println("Listing functions : \n");
		functions.keySet().forEach(name->{
			System.out.println(name+":"+functions.get(name)+"\n");
			});


	}

	public void delete(io.mark.java_examples.Executors.grpc.DeleteRequest request,
					   io.grpc.stub.StreamObserver<io.mark.java_examples.Executors.grpc.DeleteResponse> responseObserver) {
		String fxName=request.getName();
			System.out.println("Delete function received with name:"+ fxName);
				if (functions.get(fxName) == null) {
					responseObserver.onError(Status.NOT_FOUND.augmentDescription("Function does not exist").asRuntimeException());
					return;
				}
			functions.remove(fxName);
			responseObserver.onNext(DeleteResponse.newBuilder().setDeleted(true).build());
			responseObserver.onCompleted();

	}

    public void update(io.mark.java_examples.Executors.grpc.UpdateRequest request,
                       io.grpc.stub.StreamObserver<io.mark.java_examples.Executors.grpc.UpdateResponse> responseObserver) {
        // Check if it exists first
        String fxName=request.getName();
        Function fx=functions.get(fxName);
        if (fx == null) {
            responseObserver.onError(Status.NOT_FOUND.augmentDescription("Function does not exist").asRuntimeException());
            return;
        }

        System.out.println("Updating function:"+fxName);
        Function updatedFx=Function.newBuilder().setName(fxName).setHandler(request.getHandler()).setJarFile(fx.getJarFile()).build();
        functions.put(fxName,updatedFx);

        responseObserver.onNext(UpdateResponse.newBuilder().setUpdated(true).build());
        responseObserver.onCompleted();
    }

	public void get(io.mark.java_examples.Executors.grpc.GetRequest request,
					io.grpc.stub.StreamObserver<io.mark.java_examples.Executors.grpc.GetResponse> responseObserver) {
try {
	Function fx = functions.get(request.getName());
	responseObserver.onNext(GetResponse.newBuilder().setHandler(fx.getHandler()).setJarFile(fx.getJarFile()).build());
	responseObserver.onCompleted();
}catch(NullPointerException e){
	e.printStackTrace();
	responseObserver.onError(Status.NOT_FOUND.augmentDescription("Could not find function").asRuntimeException());
}
	}


	public void list(io.mark.java_examples.Executors.grpc.ListRequest request,
					 io.grpc.stub.StreamObserver<io.mark.java_examples.Executors.grpc.ListResponse> responseObserver) {
		ListResponse.Builder resBuilder = ListResponse.newBuilder();
			functions.values().forEach(function->{
                    resBuilder.addFunction(function);
		});
		responseObserver.onNext(resBuilder.build());
		responseObserver.onCompleted();
	}

@Override
	public void execute(io.mark.java_examples.Executors.grpc.ExecutionRequest request,
			         io.grpc.stub.StreamObserver<io.mark.java_examples.Executors.grpc.ExecutionResponse> responseObserver) {
	  System.out.println(request);
	  String fxName=request.getName();
	  Function fx=functions.get(fxName);
	   ExecutionResponse.Builder resBuilder = ExecutionResponse.newBuilder();

	   try {
		   ClassLoader classLoader = FunctionService.class.getClassLoader();
		   Path jarPath=Paths.get(System.getProperty("java.io.tmpdir")+"uploaded"+File.separator+fxName+File.separator+fx.getJarFile());
		   URLClassLoader urlClassLoader = new URLClassLoader(
				   new URL[]{jarPath.toUri().toURL()},
				   classLoader);
		   String[] tempArray=fx.getHandler().split(":");
		   Class executableClass = urlClassLoader.loadClass(tempArray[0]);
		   Method method;
		   Object obj1 = executableClass.newInstance();
		   Object returnedObject = null;
		   Gson gson = new GsonBuilder()
				   .setLenient()
				   .create();
		   Method[] methods = executableClass.getDeclaredMethods();
		   String methodName = tempArray[1];
		   // System.out.println(methodName);
		   boolean foundMEthod = false;
		   for (Method m : methods) {
			   if (m.getName().equals(methodName)) {
				   foundMEthod = true;
				   Class[] pTypes = m.getParameterTypes();
				   if (pTypes.length == 1) {
					   Class pType = pTypes[0];
					   method = executableClass.getMethod(methodName, new Class[]{pType});
					   returnedObject = method.invoke(obj1, gson.fromJson(request.getData(), pType));
					   break;
				   } else if (pTypes.length == 0) {
					   method = executableClass.getMethod(methodName);
					   returnedObject = method.invoke(obj1);
					   break;
				   }
			   }
		   }
		   if (foundMEthod) {
			   if (returnedObject == null) {
				   // resBuilder.setErrCode("0").build();
				   resBuilder.build();
			   } else {
				   resBuilder.setData(gson.toJson(returnedObject)).build();
			   }
			   responseObserver.onNext(resBuilder.build());
			   responseObserver.onCompleted();
		   } else {
			   responseObserver.onError(Status.UNKNOWN.augmentDescription("Could not find method").asRuntimeException());
		   }


	   }catch (JsonSyntaxException e) {
		   responseObserver.onError(Status.UNKNOWN.augmentDescription("Data provided can not be consumed by method").asRuntimeException());
		   e.printStackTrace();
	    }catch(ClassNotFoundException e){
		   responseObserver.onError(Status.UNKNOWN.augmentDescription("Could not find class").asRuntimeException());
		   e.printStackTrace();
	   }catch(NoSuchMethodException e){
		   responseObserver.onError(Status.UNKNOWN.augmentDescription("There is no such method").asRuntimeException());
		   e.printStackTrace();
	   }catch(IllegalAccessException e){
		   //resBuilder.setErrCode("Illegal access to the method");
	   }catch(InvocationTargetException e){
		   responseObserver.onError(Status.UNKNOWN.augmentDescription("Could not invoke method").asRuntimeException());
		   e.printStackTrace();
	 }catch(Exception e) {
		 responseObserver.onError(Status.UNKNOWN.augmentDescription("Exception occured").asRuntimeException());
		   e.printStackTrace();
	 }

 }

}
