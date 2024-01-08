# Code analysis
## ccs-parent 
#### Version 0.7.0-SNAPSHOT 

**By: Frederic Bregier**

*Date: 2024-01-07*

## Introduction
This document contains results of the code analysis of ccs-parent

Cloud Cloud Store Pom Parent

## Configuration

- Quality Profiles
    - Names: Sonar way [Java]; Sonar way [XML]; 
    - Files: AYrhKR9qg3TdZ7EZOd-3.json; AYrhKR_Hg3TdZ7EZOeIf.json; 


 - Quality Gate
    - Name: Sonar way
    - File: Sonar way.xml

## Synthesis

### Analysis Status

Reliability | Security | Security Review | Maintainability |
:---:|:---:|:---:|:---:
A | A | A | A |

### Quality gate status

| Quality Gate Status | OK |
|-|-|

Metric|Value
---|---
Reliability Rating on New Code|OK
Security Rating on New Code|OK
Maintainability Rating on New Code|OK
Coverage on New Code|OK
Duplicated Lines (%) on New Code|OK


### Metrics

Coverage | Duplications | Comment density | Median number of lines of code per file | Adherence to coding standard |
:---:|:---:|:---:|:---:|:---:
74.8 % | 7.9 % | 8.7 % | 88.0 | 99.4 %

### Tests

Total | Success Rate | Skipped | Errors | Failures |
:---:|:---:|:---:|:---:|:---:
595 | 100.0 % | 16 | 0 | 0

### Detailed technical debt

Reliability|Security|Maintainability|Total
---|---|---|---
-|-|0d 5h 33min|0d 5h 33min


### Metrics Range

\ | Cyclomatic Complexity | Cognitive Complexity | Lines of code per file | Coverage | Comment density (%) | Duplication (%)
:---|:---:|:---:|:---:|:---:|:---:|:---:
Min | 0.0 | 0.0 | 1.0 | 0.0 | 0.0 | 0.0
Max | 1412.0 | 886.0 | 7848.0 | 100.0 | 54.8 | 76.4

### Volume

Language|Number
---|---
Java|24512
XML|3373
Total|27885


## Issues

### Issues count by severity and types

Type / Severity|INFO|MINOR|MAJOR|CRITICAL|BLOCKER
---|---|---|---|---|---
BUG|0|0|0|0|0
VULNERABILITY|0|0|0|0|0
CODE_SMELL|35|3|22|12|1


### Issues List

Name|Description|Type|Severity|Number
---|---|---|---|---
Methods returns should not be invariant|Why is this an issue? <br /> When a method is designed to return an invariant value, it may be poor design, but it shouldn’t adversely affect the outcome of your program. <br /> However, when it happens on all paths through the logic, it is surely a bug. <br /> This rule raises an issue when a method contains several return statements that all return the same value. <br /> Noncompliant code example <br />  <br /> int foo(int a) { <br />   int b = 12; <br />   if (a == 1) { <br />     return b; <br />   } <br />   return b;  // Noncompliant <br /> } <br /> |CODE_SMELL|BLOCKER|1
Fields in a "Serializable" class should either be transient or serializable|This rule raises an issue on a non-transient and non-serializable field within a serializable class, if said class does not have <br /> writeObject and readObject methods defined. <br /> Why is this an issue? <br /> By contract, fields in a Serializable class must themselves be either Serializable or transient. Even if the <br /> class is never explicitly serialized or deserialized, it is not safe to assume that this cannot happen. For instance, under load, most J2EE <br /> application frameworks flush objects to disk. <br /> An object that implements Serializable but contains non-transient, non-serializable data members (and thus violates the contract) <br /> could cause application crashes and open the door to attackers. In general, a Serializable class is expected to fulfil its contract and <br /> not exhibit unexpected behaviour when an instance is serialized. <br /> This rule raises an issue on: <br />  <br />    non-Serializable fields,  <br />    collection fields when they are not private (because they could be assigned non-Serializable values externally), <br />    <br />    when a field is assigned a non-Serializable type within the class.  <br />  <br /> How to fix it <br /> Consider the following scenario. <br />  <br /> public class Address { <br />     ... <br /> } <br />  <br /> public class Person implements Serializable { <br />   private static final long serialVersionUID = 1905122041950251207L; <br />  <br />   private String name; <br />   private Address address;  // Noncompliant, Address is not serializable <br /> } <br />  <br /> How to fix this issue depends on the application’s needs. If the field’s value should be preserved during serialization and deserialization, you <br /> may want to make the field’s value serializable. <br />  <br /> public class Address implements Serializable { <br />   private static final long serialVersionUID = 2405172041950251807L; <br />  <br />     ... <br /> } <br />  <br /> public class Person implements Serializable { <br />   private static final long serialVersionUID = 1905122041950251207L; <br />  <br />   private String name; <br />   private Address address; // Compliant, Address is serializable <br /> } <br />  <br /> If the field’s value does not need to be preserved during serialization and deserialization, mark it as transient. The field will be <br /> ignored when the object is serialized. After deserialization, the field will be set to the default value corresponding to its type (e.g., <br /> null for object references). <br />  <br /> public class Address { <br />     ... <br /> } <br />  <br /> public class Person implements Serializable { <br />   private static final long serialVersionUID = 1905122041950251207L; <br />  <br />   private String name; <br />   private transient Address address; // Compliant, the field is transient <br /> } <br />  <br /> The alternative to making all members serializable or transient is to implement special methods which take on the responsibility of <br /> properly serializing and de-serializing the object writeObject and readObject. These methods can be used to properly <br /> (de-)serialize an object, even though it contains fields that are not transient or serializable. Hence, this rule does not raise issues on fields of <br /> classes which implement these methods. <br />  <br /> public class Address { <br />     ... <br /> } <br />  <br /> public class Person implements Serializable { <br />   private static final long serialVersionUID = 1905122041950251207L; <br />  <br />   private String name; <br />   private Address address; // Compliant, writeObject and readObject handle this field <br />  <br />   private void writeObject(java.io.ObjectOutputStream out) throws IOException { <br />     // Appropriate serialization logic here <br />   } <br />  <br />   private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException { <br />     // Appropriate deserialization logic here <br />   } <br /> } <br />  <br /> Resources <br />  <br />    Saving Unserializable Objects to Disk - MITRE, CWE-594  <br />    Interface Serializable - Java SE 11 API <br />   Documentation  <br />    Interface Serializable - Java SE 17 API <br />   Documentation  <br /> |CODE_SMELL|CRITICAL|1
"static" base class members should not be accessed via derived types|Why is this an issue? <br /> In the interest of code clarity, static members of a base class should never be accessed using a derived type’s name. <br /> Doing so is confusing and could create the illusion that two different static members exist. <br /> Noncompliant code example <br />  <br /> class Parent { <br />   public static int counter; <br /> } <br />  <br /> class Child extends Parent { <br />   public Child() { <br />     Child.counter++;  // Noncompliant <br />   } <br /> } <br />  <br /> Compliant solution <br />  <br /> class Parent { <br />   public static int counter; <br /> } <br />  <br /> class Child extends Parent { <br />   public Child() { <br />     Parent.counter++; <br />   } <br /> } <br /> |CODE_SMELL|CRITICAL|1
Cognitive Complexity of methods should not be too high|Why is this an issue? <br /> Cognitive Complexity is a measure of how hard the control flow of a method is to understand. Methods with high Cognitive Complexity will be <br /> difficult to maintain. <br /> Exceptions <br /> equals and hashCode methods are ignored because they might be automatically generated and might end up being difficult to <br /> understand, especially in the presence of many fields. <br /> Resources <br /> Documentation <br />  <br />    Cognitive Complexity  <br /> |CODE_SMELL|CRITICAL|10
Track uses of "TODO" tags|Why is this an issue? <br /> Developers often use TOOO tags to mark areas in the code where additional work or improvements are needed but are not implemented <br /> immediately. However, these TODO tags sometimes get overlooked or forgotten, leading to incomplete or unfinished code. This code smell <br /> class aims to identify and address such unattended TODO tags to ensure a clean and maintainable codebase. This description will explore <br /> why this is a problem and how it can be fixed to improve the overall code quality. <br /> What is the potential impact? <br /> Unattended TODO tags in code can have significant implications for the development process and the overall codebase. <br /> Incomplete Functionality: When developers leave TODO tags without implementing the corresponding code, it results in incomplete <br /> functionality within the software. This can lead to unexpected behavior or missing features, adversely affecting the end-user experience. <br /> Missed Bug Fixes: If developers do not promptly address TODO tags, they might overlook critical bug fixes and security updates. <br /> Delayed bug fixes can result in more severe issues and increase the effort required to resolve them later. <br /> Impact on Collaboration: In team-based development environments, unattended TODO tags can hinder collaboration. Other team members <br /> might not be aware of the intended changes, leading to conflicts or redundant efforts in the codebase. <br /> Codebase Bloat: Accumulation of unattended TODO tags over time can clutter the codebase and make it difficult to distinguish between <br /> work in progress and completed code. This bloat can make it challenging to maintain an organized and efficient codebase. <br /> Addressing this code smell is essential to ensure a maintainable, readable, reliable codebase and promote effective collaboration among <br /> developers. <br /> Noncompliant code example <br />  <br /> void doSomething() { <br />   // TODO <br /> } <br />  <br /> Resources <br />  <br />    MITRE, CWE-546 - Suspicious Comment  <br /> |CODE_SMELL|INFO|34
Methods should not perform too many tasks (aka Brain method)|Methods should not perform too many tasks (Brain Method). <br /> Why is this an issue? <br /> This issue is raised when Sonar considers that a method is a 'Brain Method'.  A Brain Method is a method that tends to centralize its owner’s <br /> class logic and generally performs too many operations. This can include checking too many conditions, using lots of variables, and ultimately making <br /> it difficult to understand, maintain and reuse. It is characterized by high LOC number, high cyclomatic and cognitive complexity, and a large <br /> number of variables being used. <br /> What is the potential impact? <br /> Brain Methods are often hard to cover with tests, because of their deep nesting, and they are error-prone, because of the many local variables they <br /> usually introduce. Such methods will be very hard to read and understand for anyone outside who created them, and therefore hard to maintain and fix <br /> if bugs get spotted. They also enable code duplication since the method itself can hardly be reused anywhere else. <br /> How to fix it <br /> The common approach is to identify fragments of the method’s code that deal with a specific responsibility and extract them to a new method. This <br /> will make each method more readable, easy to understand and maintain, easier to test, and more prone to be reused. In this paper, the authors <br /> describe a systematic procedure to refactor this type of code smell: "Assessing the Refactoring of <br /> Brain Methods". <br /> Code examples <br /> Noncompliant code example <br />  <br /> void farmDailyRoutine() { <br />     Crops southEastCrops = getCrops(1, -1); <br />     Crops eastCrops = getCrops(1, 0); <br />     WaterContainer waterContainer = new WaterContainer(); <br />     List&lt;Bottle&gt; bottles = new ArrayList&lt;&gt;(); <br />     for(int i = 0; i &lt; 10; i++) { <br />         var bottle = new Bottle(); <br />         bottle.addWater(10L); <br />         bottle.putCap(); <br />         bottle.shake(2); <br />         bottles.add(bottle); <br />     } <br />     waterContainer.store(bottles); <br />  <br />     Truck t1 = new Truck(Truck.Type.TRANSPORT); <br />     t1.load(waterContainer); <br />     if(Weather.current != Weather.RAINY) { <br />         WaterContainer extraWaterContainer = new WaterContainer(); <br />         List&lt;Bottle&gt; extraBottles = new ArrayList&lt;&gt;(); <br />         if(southEastCrops.isDry()) { <br />             for(LandSlot ls : southEastCrops.lands()) { <br />                 Bottle b = new Bottle(); <br />                 b.addWater(10L); <br />                 b.putCap(); <br />                 extraBottles.add(b); <br />             } <br />         } else { <br />             extraBottles.add(new Bottle()); <br />         } <br />         if(eastCrops.isDry()) { <br />             for(LandSlot ls : southEastCrops.lands()) { <br />                 Bottle b = new Bottle(); <br />                 b.addWater(10L); <br />                 b.putCap(); <br />                 extraBottles.add(b); <br />             } <br />         } else { <br />             extraBottles.add(new Bottle()); <br />         } <br />         extraWaterContainer.store(extraBottles); <br />         t1.load(extraWaterContainer); <br />     } else { <br />         WaterContainer extraWaterContainer = WaterSource.clone(waterContainer); <br />         t1.load(extraWaterContainer) <br />     } <br /> } <br />  <br /> Compliant solution <br />  <br /> void farmDailyRoutine() { // Compliant: Simpler method, making use of extracted and distributed logic <br />     Crops southEastCrops = getCrops(1, -1); <br />     Crops eastCrops = getCrops(1, 0); <br />     WaterContainer waterContainer = new WaterContainer(); <br />     List&lt;Bottle&gt; bottles = getWaterBottles(10, 10L, true); <br />     waterContainer.store(bottles); <br />  <br />     Truck t1 = new Truck(Truck.Type.TRANSPORT); <br />     t1.load(waterContainer); <br />     if(Weather.current != Weather.RAINY) { <br />         WaterContainer extraWaterContainer = new WaterContainer(); <br />         fillContainerForCrops(extraWaterContainer, southEastCrops); <br />         fillContainerForCrops(extraWaterContainer, eastCrops); <br />         t1.load(extraWaterContainer); <br />     } else { <br />         WaterContainer extraWaterContainer = WaterSource.clone(waterContainer); <br />         t1.load(extraWaterContainer) <br />     } <br /> } <br />  <br /> private fillContainerForCrops(WaterContainer wc, Crops crops) { // Compliant: extracted readable and reusable method <br />     if(crops.isDry()) { <br />         wc.store(getWaterBottles(crops.lands().size(), 10L, false)); <br />     } else { <br />         wc.store(Collections.singleton(new Bottle())); <br />     } <br /> } <br />  <br /> private List&lt;Bottle&gt; getWaterBottles(int qt, long liquid, boolean shake){ // Compliant: extracted readable and reusable method <br />     List&lt;Bottle&gt; bottles = new ArrayList&lt;&gt;(); <br />     for(int i = 0; i &lt; qt; i++) { <br />         Bottle b = new Bottle(); <br />         b.addWater(liquid); <br />         b.putCap(); <br />         if(shake) { <br />             b.shake(); <br />         } <br />         bottles.add(b); <br />     } <br />     return bottles; <br /> } <br />  <br /> How does this work? <br /> In this case, the method farmDailyRoutine was taking care of performing many different tasks, with nested conditions and loops, it was <br /> long and had plenty of local variables. By separating its logic into multiple single-responsibility methods, it is reusing parts of its original <br /> duplicated code and each of the new methods is now readable and easy to understand. They are now also easier to cover with tests, and many other parts <br /> of the owner class could benefit from using these methods. <br /> Resources <br /> Articles &amp; blog posts <br />  <br />    "Object-Oriented Metrics in Practice: Using Software Metrics to Characterize, <br />   Evaluate, and Improve the Design of Object-Oriented Systems" by M. Lanza, R. Marinescu   <br />    "Assessing the Refactoring of Brain Methods" by S. Vidal, I. Berra, S. Zulliani, C. <br />   Marcos, J. A. Diaz Pace   <br /> |CODE_SMELL|INFO|1
Generic exceptions should never be thrown|Why is this an issue? <br /> Using such generic exceptions as Error, RuntimeException, Throwable, and Exception prevents <br /> calling methods from handling true, system-generated exceptions differently than application-generated errors. <br /> Noncompliant code example <br />  <br /> public void foo(String bar) throws Throwable {  // Noncompliant <br />   throw new RuntimeException("My Message");     // Noncompliant <br /> } <br />  <br /> Compliant solution <br />  <br /> public void foo(String bar) { <br />   throw new MyOwnRuntimeException("My Message"); <br /> } <br />  <br /> Exceptions <br /> Generic exceptions in the signatures of overriding methods are ignored, because overriding method has to follow signature of the throw declaration <br /> in the superclass. The issue will be raised on superclass declaration of the method (or won’t be raised at all if superclass is not part of the <br /> analysis). <br />  <br /> @Override <br /> public void myMethod() throws Exception {...} <br />  <br /> Generic exceptions are also ignored in the signatures of methods that make calls to methods that throw generic exceptions. <br />  <br /> public void myOtherMethod throws Exception { <br />   doTheThing();  // this method throws Exception <br /> } <br />  <br /> Resources <br />  <br />    MITRE, CWE-397 - Declaration of Throws for Generic Exception  <br />    CERT, ERR07-J. - Do not throw RuntimeException, Exception, or Throwable  <br /> |CODE_SMELL|MAJOR|2
Track uses of "FIXME" tags|Why is this an issue? <br /> FIXME tags are commonly used to mark places where a bug is suspected, but which the developer wants to deal with later. <br /> Sometimes the developer will not have the time or will simply forget to get back to that tag. <br /> This rule is meant to track those tags and to ensure that they do not go unnoticed. <br />  <br /> int divide(int numerator, int denominator) { <br />   return numerator / denominator;              // FIXME denominator value might be  0 <br /> } <br />  <br /> Resources <br /> Documentation <br />  <br />    MITRE, CWE-546 - Suspicious Comment  <br /> |CODE_SMELL|MAJOR|13
Sections of code should not be commented out|Why is this an issue? <br /> Programmers should not comment out code as it bloats programs and reduces readability. <br /> Unused code should be deleted and can be retrieved from source control history if required.|CODE_SMELL|MAJOR|2
JUnit4 @Ignored and JUnit5 @Disabled annotations should be used to disable tests and should provide a rationale|Why is this an issue? <br /> When a test fails due, for example, to infrastructure issues, you might want to ignore it temporarily. But without some kind of notation about why <br /> the test is being ignored, it may never be reactivated. Such tests are difficult to address without comprehensive knowledge of the project, and end up <br /> polluting their projects. <br /> This rule raises an issue for each ignored test that does not have any comment about why it is being skipped. <br />  <br />    For Junit4, this rule targets the @Ignore annotation.  <br />    For Junit5, this rule targets the @Disabled annotation.  <br />    Cases where assumeTrue(false) or assumeFalse(true) are used to skip tests are targeted as well.  <br />  <br /> Noncompliant code example <br />  <br /> @Ignore  // Noncompliant <br /> @Test <br /> public void testDoTheThing() { <br />   // ... <br />  <br /> or <br />  <br /> @Test <br /> public void testDoTheThing() { <br />   Assume.assumeFalse(true); // Noncompliant <br />   // ... <br />  <br /> Compliant solution <br />  <br /> @Test <br /> @Ignore("See Ticket #1234") <br /> public void testDoTheThing() { <br />   // ... <br /> |CODE_SMELL|MAJOR|4
Reflection should not be used to increase accessibility of classes, methods, or fields|Why is this an issue? <br /> Altering or bypassing the accessibility of classes, methods, or fields through reflection violates the encapsulation principle. This can break the <br /> internal contracts of the accessed target and lead to maintainability issues and runtime errors. <br /> This rule raises an issue when reflection is used to change the visibility of a class, method or field, and when it is used to directly update a <br /> field value. <br />  <br /> public void makeItPublic(String methodName) throws NoSuchMethodException { <br />  <br />   this.getClass().getMethod(methodName).setAccessible(true); // Noncompliant <br /> } <br />  <br /> public void setItAnyway(String fieldName, int value) { <br />   this.getClass().getDeclaredField(fieldName).setInt(this, value); // Noncompliant; bypasses controls in setter <br /> } <br />  <br /> Resources <br /> Documentation <br />  <br />    Wikipedia definition of Encapsulation  <br />    CERT, SEC05-J. - Do not use reflection to increase accessibility of classes, <br />   methods, or fields  <br /> |CODE_SMELL|MAJOR|1
"switch" statements should have at least 3 "case" clauses|Why is this an issue? <br /> switch statements are useful when there are many different cases depending on the value of the same expression. <br /> For just one or two cases however, the code will be more readable with if statements. <br /> Noncompliant code example <br />  <br /> switch (variable) { <br />   case 0: <br />     doSomething(); <br />     break; <br />   default: <br />     doSomethingElse(); <br />     break; <br /> } <br />  <br /> Compliant solution <br />  <br /> if (variable == 0) { <br />   doSomething(); <br /> } else { <br />   doSomethingElse(); <br /> } <br /> |CODE_SMELL|MINOR|1
Loops should not contain more than a single "break" or "continue" statement|Why is this an issue? <br /> The use of break and continue statements increases the complexity of the control flow and makes it harder to understand <br /> the program logic. In order to keep a good program structure, they should not be applied more than once per loop. <br /> This rule reports an issue when there is more than one break or continue statement in a loop. The code should be <br /> refactored to increase readability if there is more than one. <br /> Noncompliant code example <br />  <br /> for (int i = 1; i &lt;= 10; i++) {     // Noncompliant; two "continue" statements <br />   if (i % 2 == 0) { <br />     continue; <br />   } <br />  <br />   if (i % 3 == 0) { <br />     continue; <br />   } <br />   // ... <br /> } <br />  <br /> Compliant solution <br />  <br /> for (int i = 1; i &lt;= 10; i++) { <br />   if (i % 2 == 0 &#124&#124 i % 3 == 0) { <br />     continue; <br />   } <br />   // ... <br /> } <br />  <br /> Resources <br /> Documentation <br />  <br />    Oracle - Labeled Statements  <br />  <br /> Articles &amp; blog posts <br />  <br />    StackExchange - Java labels. To <br />   be or not to be  <br />    StackOverflow - Labels in Java - bad practice?  <br /> |CODE_SMELL|MINOR|1
"@Deprecated" code should not be used|Why is this an issue? <br /> Once deprecated, classes, and interfaces, and their members should be avoided, rather than used, inherited or extended. Deprecation is a warning <br /> that the class or interface has been superseded, and will eventually be removed. The deprecation period allows you to make a smooth transition away <br /> from the aging, soon-to-be-retired technology. <br /> Noncompliant code example <br />  <br /> /** <br />  * @deprecated  As of release 1.3, replaced by {@link #Fee} <br />  */ <br /> @Deprecated <br /> public class Fum { ... } <br />  <br /> public class Foo { <br />   /** <br />    * @deprecated  As of release 1.7, replaced by {@link #doTheThingBetter()} <br />    */ <br />   @Deprecated <br />   public void doTheThing() { ... } <br />  <br />   public void doTheThingBetter() { ... } <br /> } <br />  <br /> public class Bar extends Foo { <br />   public void doTheThing() { ... } // Noncompliant; don't override a deprecated method or explicitly mark it as @Deprecated <br /> } <br />  <br /> public class Bar extends Fum {  // Noncompliant; Fum is deprecated <br />  <br />   public void myMethod() { <br />     Foo foo = new Foo();  // okay; the class isn't deprecated <br />     foo.doTheThing();  // Noncompliant; doTheThing method is deprecated <br />   } <br /> } <br />  <br /> Resources <br />  <br />    MITRE, CWE-477 - Use of Obsolete Functions  <br />    CERT, MET02-J. - Do not use deprecated or obsolete classes or methods  <br /> |CODE_SMELL|MINOR|1


## Security Hotspots

### Security hotspots count by category and priority

Category / Priority|LOW|MEDIUM|HIGH
---|---|---|---
LDAP Injection|0|0|0
Object Injection|0|0|0
Server-Side Request Forgery (SSRF)|0|0|0
XML External Entity (XXE)|0|0|0
Insecure Configuration|0|0|0
XPath Injection|0|0|0
Authentication|0|0|0
Weak Cryptography|0|0|0
Denial of Service (DoS)|0|0|0
Log Injection|0|0|0
Cross-Site Request Forgery (CSRF)|0|0|0
Open Redirect|0|0|0
Permission|0|0|0
SQL Injection|0|0|0
Encryption of Sensitive Data|0|0|0
Traceability|0|0|0
Buffer Overflow|0|0|0
File Manipulation|0|0|0
Code Injection (RCE)|0|0|0
Cross-Site Scripting (XSS)|0|0|0
Command Injection|0|0|0
Path Traversal Injection|0|0|0
HTTP Response Splitting|0|0|0
Others|0|0|0


### Security hotspots

