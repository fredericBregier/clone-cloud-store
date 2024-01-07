POM Version management
#######################

In order to maintain as much as possible the simplicity and compatibility, here are some
rules:

- Version is defined statically in the highest Pom (also named pom parent but in the same project)
- version is defined statically in all sub pom (child poms) for reference to Parent
- In highest POM (parent pom of the project):

  - Use ``dependencyManagement`` to define all modules version from this project, but use as version ${project.version}

    - Place at first in this management the quarkus.platform pom with scope import

  - If needed, you can add extra dependencies there, to specify version in top Pom

- In sub POM, you shall define real ``dependencies`` this time, but with no version, since
  they shall be managed by the parent POM

To update the version of all project, use the **versions-maven-plugin**:

.. code-block:: shell
  :caption: Example for **versions-maven-plugin**

  mvn versions:set -DnewVersion=x.y.z-SNAPSHOT
  mvn versions:set -DnewVersion=x.y.z
  mvn versions:revert
  mvn versions:commit

After using *versions:set* command, you can check if the result is correct (for instance
using ``git diff``).

If OK, then commit it (it will simply remove the backup file).

If KO, then revert and redo (it will replace the current pom with the backup one).

Note that it will update all modules recursively in the project.
