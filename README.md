wicca
==========

RESTful Web Service built on Jersey 2 and Datastax CQL3 driver
If you are implementing a new Service, check the wicca-example project. You can declare the parent of the pom of your
project as jesse for convenience if you want to keep your own pom clean of plugins like the Maven war plugin. It also
provides a jetty plugin so running "maven jetty:run" will startup Jetty on http://localhost:8080 making testing your
app easy. Include wicca-core a dependency.

Dependencies already provided in wicca-core:
Netty
LogBack
javax-rs
Google Guava
Jersey 2
Datastax Driver
javax.Servlet

Maven Plugins already provided in wicca:
Jetty Maven
Maven War

Unless you override CassandraApplication or declare you packages in a web.xml, your Jersey Resource classes should be in
the package net.disbelieve.wicca.jersey.service

You may provide a web.xml, but none is required. To configure your application, extending
net.disbelieve.wicca.jersey.CassandraApplication is preferred.
