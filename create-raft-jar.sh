
rm -f Server*.log
javac -cp "src:lib/log4j/*" -proc:none src/RaftServer.java
jar cfm RaftServer.jar MANIFEST.MF -C src/ . 
