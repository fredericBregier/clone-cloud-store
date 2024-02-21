# Code analysis
## ccs-parent 
#### Version 0.8.0-SNAPSHOT 

**By: Frederic Bregier**

*Date: 2024-02-21*

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
Security Hotspots Reviewed on New Code|OK


### Metrics

Coverage | Duplications | Comment density | Median number of lines of code per file | Adherence to coding standard |
:---:|:---:|:---:|:---:|:---:
78.5 % | 4.6 % | 7.8 % | 80.5 | 99.7 %

### Tests

Total | Success Rate | Skipped | Errors | Failures |
:---:|:---:|:---:|:---:|:---:
696 | 100.0 % | 0 | 0 | 0

### Detailed technical debt

Reliability|Security|Maintainability|Total
---|---|---|---
-|-|0d 3h 27min|0d 3h 27min


### Metrics Range

\ | Cyclomatic Complexity | Cognitive Complexity | Lines of code per file | Coverage | Comment density (%) | Duplication (%)
:---|:---:|:---:|:---:|:---:|:---:|:---:
Min | 0.0 | 0.0 | 1.0 | 0.0 | 0.0 | 0.0
Max | 1367.0 | 907.0 | 9457.0 | 100.0 | 55.6 | 68.1

### Volume

Language|Number
---|---
Java|29169
XML|3327
Total|32496


## Issues

### Issues count by severity and types

Type / Severity|INFO|MINOR|MAJOR|CRITICAL|BLOCKER
---|---|---|---|---|---
BUG|0|0|0|0|0
VULNERABILITY|0|0|0|0|0
CODE_SMELL|13|2|15|12|0


### Issues List

Name|Description|Type|Severity|Number
---|---|---|---|---
"static" base class members should not be accessed via derived types|Why is this an issue? <br /> In the interest of code clarity, static members of a base class should never be accessed using a derived type’s name. <br /> Doing so is confusing and could create the illusion that two different static members exist. <br /> Noncompliant code example <br />  <br /> class Parent { <br />   public static int counter; <br /> } <br />  <br /> class Child extends Parent { <br />   public Child() { <br />     Child.counter++;  // Noncompliant <br />   } <br /> } <br />  <br /> Compliant solution <br />  <br /> class Parent { <br />   public static int counter; <br /> } <br />  <br /> class Child extends Parent { <br />   public Child() { <br />     Parent.counter++; <br />   } <br /> } <br /> |CODE_SMELL|CRITICAL|1
Cognitive Complexity of methods should not be too high|Why is this an issue? <br /> Cognitive Complexity is a measure of how hard the control flow of a method is to understand. Methods with high Cognitive Complexity will be <br /> difficult to maintain. <br /> Exceptions <br /> equals and hashCode methods are ignored because they might be automatically generated and might end up being difficult to <br /> understand, especially in the presence of many fields. <br /> Resources <br /> Documentation <br />  <br />    Cognitive Complexity  <br /> |CODE_SMELL|CRITICAL|11
Track uses of "TODO" tags|Why is this an issue? <br /> Developers often use TOOO tags to mark areas in the code where additional work or improvements are needed but are not implemented <br /> immediately. However, these TODO tags sometimes get overlooked or forgotten, leading to incomplete or unfinished code. This code smell <br /> class aims to identify and address such unattended TODO tags to ensure a clean and maintainable codebase. This description will explore <br /> why this is a problem and how it can be fixed to improve the overall code quality. <br /> What is the potential impact? <br /> Unattended TODO tags in code can have significant implications for the development process and the overall codebase. <br /> Incomplete Functionality: When developers leave TODO tags without implementing the corresponding code, it results in incomplete <br /> functionality within the software. This can lead to unexpected behavior or missing features, adversely affecting the end-user experience. <br /> Missed Bug Fixes: If developers do not promptly address TODO tags, they might overlook critical bug fixes and security updates. <br /> Delayed bug fixes can result in more severe issues and increase the effort required to resolve them later. <br /> Impact on Collaboration: In team-based development environments, unattended TODO tags can hinder collaboration. Other team members <br /> might not be aware of the intended changes, leading to conflicts or redundant efforts in the codebase. <br /> Codebase Bloat: Accumulation of unattended TODO tags over time can clutter the codebase and make it difficult to distinguish between <br /> work in progress and completed code. This bloat can make it challenging to maintain an organized and efficient codebase. <br /> Addressing this code smell is essential to ensure a maintainable, readable, reliable codebase and promote effective collaboration among <br /> developers. <br /> Noncompliant code example <br />  <br /> void doSomething() { <br />   // TODO <br /> } <br />  <br /> Resources <br />  <br />    MITRE, CWE-546 - Suspicious Comment  <br /> |CODE_SMELL|INFO|13
Unused "private" fields should be removed|Why is this an issue? <br /> If a private field is declared but not used in the program, it can be considered dead code and should therefore be removed. This will <br /> improve maintainability because developers will not wonder what the variable is used for. <br /> Note that this rule does not take reflection into account, which means that issues will be raised on private fields that are only <br /> accessed using the reflection API. <br /> Noncompliant code example <br />  <br /> public class MyClass { <br />   private int foo = 42; <br />  <br />   public int compute(int a) { <br />     return a * 42; <br />   } <br />  <br /> } <br />  <br /> Compliant solution <br />  <br /> public class MyClass { <br />   public int compute(int a) { <br />     return a * 42; <br />   } <br /> } <br />  <br /> Exceptions <br /> The rule admits 3 exceptions: <br />  <br />    Serialization id fields  <br />  <br /> The Java serialization runtime associates with each serializable class a version number, called serialVersionUID, which is used during <br /> deserialization to verify that the sender and receiver of a serialized object have loaded classes for that object that are compatible with respect to <br /> serialization. <br /> A serializable class can declare its own serialVersionUID explicitly by declaring a field named serialVersionUID that <br /> must be static, final, and of type long. By definition those serialVersionUID fields should not be reported by this rule: <br />  <br /> public class MyClass implements java.io.Serializable { <br />   private static final long serialVersionUID = 42L; <br /> } <br />  <br />  <br />    Annotated fields  <br />  <br /> The unused field in this class will not be reported by the rule as it is annotated. <br />  <br /> public class MyClass { <br />   @SomeAnnotation <br />   private int unused; <br /> } <br />  <br />  <br />    Fields from classes with native methods  <br />  <br /> The unused field in this class will not be reported by the rule as it might be used by native code. <br />  <br /> public class MyClass { <br />   private int unused = 42; <br />   private native static void doSomethingNative(); <br /> } <br /> |CODE_SMELL|MAJOR|5
Track uses of "FIXME" tags|Why is this an issue? <br /> FIXME tags are commonly used to mark places where a bug is suspected, but which the developer wants to deal with later. <br /> Sometimes the developer will not have the time or will simply forget to get back to that tag. <br /> This rule is meant to track those tags and to ensure that they do not go unnoticed. <br />  <br /> int divide(int numerator, int denominator) { <br />   return numerator / denominator;              // FIXME denominator value might be  0 <br /> } <br />  <br /> Resources <br /> Documentation <br />  <br />    MITRE, CWE-546 - Suspicious Comment  <br /> |CODE_SMELL|MAJOR|7
Sections of code should not be commented out|Why is this an issue? <br /> Programmers should not comment out code as it bloats programs and reduces readability. <br /> Unused code should be deleted and can be retrieved from source control history if required.|CODE_SMELL|MAJOR|2
Reflection should not be used to increase accessibility of classes, methods, or fields|Why is this an issue? <br /> Altering or bypassing the accessibility of classes, methods, or fields through reflection violates the encapsulation principle. This can break the <br /> internal contracts of the accessed target and lead to maintainability issues and runtime errors. <br /> This rule raises an issue when reflection is used to change the visibility of a class, method or field, and when it is used to directly update a <br /> field value. <br />  <br /> public void makeItPublic(String methodName) throws NoSuchMethodException { <br />  <br />   this.getClass().getMethod(methodName).setAccessible(true); // Noncompliant <br /> } <br />  <br /> public void setItAnyway(String fieldName, int value) { <br />   this.getClass().getDeclaredField(fieldName).setInt(this, value); // Noncompliant; bypasses controls in setter <br /> } <br />  <br /> Resources <br /> Documentation <br />  <br />    Wikipedia definition of Encapsulation  <br />    CERT, SEC05-J. - Do not use reflection to increase accessibility of classes, <br />   methods, or fields  <br /> |CODE_SMELL|MAJOR|1
"@Deprecated" code should not be used|Why is this an issue? <br /> Once deprecated, classes, and interfaces, and their members should be avoided, rather than used, inherited or extended. Deprecation is a warning <br /> that the class or interface has been superseded, and will eventually be removed. The deprecation period allows you to make a smooth transition away <br /> from the aging, soon-to-be-retired technology. <br /> Noncompliant code example <br />  <br /> /** <br />  * @deprecated  As of release 1.3, replaced by {@link #Fee} <br />  */ <br /> @Deprecated <br /> public class Fum { ... } <br />  <br /> public class Foo { <br />   /** <br />    * @deprecated  As of release 1.7, replaced by {@link #doTheThingBetter()} <br />    */ <br />   @Deprecated <br />   public void doTheThing() { ... } <br />  <br />   public void doTheThingBetter() { ... } <br /> } <br />  <br /> public class Bar extends Foo { <br />   public void doTheThing() { ... } // Noncompliant; don't override a deprecated method or explicitly mark it as @Deprecated <br /> } <br />  <br /> public class Bar extends Fum {  // Noncompliant; Fum is deprecated <br />  <br />   public void myMethod() { <br />     Foo foo = new Foo();  // okay; the class isn't deprecated <br />     foo.doTheThing();  // Noncompliant; doTheThing method is deprecated <br />   } <br /> } <br />  <br /> Resources <br />  <br />    MITRE, CWE-477 - Use of Obsolete Functions  <br />    CERT, MET02-J. - Do not use deprecated or obsolete classes or methods  <br /> |CODE_SMELL|MINOR|2


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

