artemis-core
==========

RESTful Web SErvice built on Jersey 2 and CQL3

If you are implementing a new Service, check the artemis-example project. The top level pom in your project must declare artemis-core as it's parent.  

Dependencies already provided:
* Netty
* LogBack
* javax-rs
* Google Guava
* Jersey 2
* Datastax Driver
* javax.Servlet

Maven Plugins already provided:
* Jetty Maven
* Maven War

Your Jersey Resource classes must be in the package:
net.disbelieve.artemis.jersey.resource

You may provide a web.xml, but none is required. To configure your application, extending net.disbelieve.artemis.jersey.ArtemisApplication
is preferred.
