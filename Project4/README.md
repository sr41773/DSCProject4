CSCI 6780: Distributed Computing Systems
=================================================
Project 4: Consistent Hashin-based Naming Service
========================================

(a.)    Mrudang Patel          
        Shriya Rasale

(b.) File structure:
    Project4 
            -- src
                -- hashing
                    -- BootstrapServer.java
                    -- NameServer.java
            -- bin
                -- hashing
                    -- BootstrapServer.class
                    -- BootstrapServer$ServerHandler.class
                    -- NameServer.class
                    -- ServerInfo.class
            -- config
                --bnconfig_example.txt
                -- bnComfigFile.txt
                -- nsconfig_example.txt
                -- nsConfigFile.txt
            -- README.md
            -- Programming-Project4.pdf

> To compile the files:
under Project 4 (root) directory:
javac -d bin -sourcepath src src\hashing\*.java

> To run Bootstrap:
java -cp bin hashing.BootstrapServer config\bnConfigFile.txt

> To run Name Server (on a different terminal):
java -cp bin hashing.NameServer   config\nsConfigFile.txt

(c.)
This project was done in its entirety by Mrudang Patel and Shriya Rasale. We hereby 
state that we have not received unauthorized help of any form.
 

