
javac -cp "src:lib/log4j/*" -proc:none src/Server.java
jar cfm RaftServer.jar Manifest.txt -C src/ . 
