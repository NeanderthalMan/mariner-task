Hello Mariner folks!

- src/        You can find source code here
- test/       unittests
- datamerge/  original mariner task files
- jarlib/     support library binaries

To run the program:
- Boot up eclipse
- Go File->Import->General->Existing Projects into Workspace, and click Browse to select your local folder. Proceed to Finish.
- With the project opened, navigate to src/mariner/task/MergeTask, and right click the class. Click Run As->Java Application. 

This will process the report.* files provided and found under /datamerge, and produce the output ./merge-report.csv. The program will overwrite the merge-report.csv with each run.
The service-guid counts are printed to sysout and should be readily visible in your console.

- Unittests can be run similarly by navigating to test/mariner/task/MergeTask, and right click the class. Click Run As->JUnit Test.


Explanation for libraries used:
- I pulled in the lastest jackson libs for parsing the xml and json into Java8 Streams
- commons-io was used for simple file IO
- Beyond that it was just the junit4 lib which had hamcrest as a dependency. 

