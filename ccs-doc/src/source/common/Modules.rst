Modules
#######

In order to try to keep modular as much as possible, close to an Hexagonal architecture, and to restrict
as much as possible the dependencies for external applications using this solution, the following
modules are proposed:

* Client side and Server side:

  * **standard**: to hold generic extensions as Guid, Multiple Actions InputStream, Zstd InputStreams,
    Cipher InputStream, Stream and Iterator utils or ParametersChecker with no Quarkus dependencies

    * Almost all modules depend on this one

  * **quarkus**: to hold extensions for Quarkus until native support comes to Quarkus (in
    particular efficient InputStream support for both POST and GET using reactive API) and
    to hold generic extensions as Chunked InputStream, Priority Queue, State Machine or Tea InputStream
    with Quarkus dependencies or the service identification for CCS

    * Almost all modules depend on this one
    * It relies on current patch Quarkus-patch-client which handles InputStream, until Quarkus fill the gap

  * **quarkus-server**: to hold extensions for Quarkus for Server part

    * It relies on current patch Quarkus-patch-client which handles InputStream, until Quarkus fill the gap

  * **database**: to hold the DB extension using Panache

.. graphviz::
  :caption: Dependencies Graph for Cloud Cloud Store Common

  digraph dependencies {
    subgraph {
      "standard" [color = gray]; "quarkus"  [color = gray]; "quarkus-server"  [color = gray];
    }
    subgraph {
      "database" [color = brown];
    }

    "standard" -> {"quarkus" "database"};
    "quarkus" -> {"quarkus-server" "database"};
    "quarkus-server" -> "CcsService";
    "database" -> "CcsService";
  }
