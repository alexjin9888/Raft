
rm -f Server*.log

javac -cp "src:lib/log4j/*" -proc:none src/RaftServer.java
jar cfm RaftServer.jar MANIFEST-SERVER.MF -C src/ .

javac -cp "src:lib/log4j/*" -proc:none src/RaftClient.java
jar cfm RaftClient.jar MANIFEST-CLIENT.MF -C src/ .
