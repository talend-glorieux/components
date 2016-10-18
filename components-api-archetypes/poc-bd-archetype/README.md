maven-library-archetype
=================

Maven archetype 
To create the archetype: 
Go into the folder "components-archetype"
And use: "mvn clean install"

Then you can use the command :
\n mvn archetype:generate -DarchetypeArtifactId=talendTcomp -DarchetypeGroupId=org.talend.components -DgroupId=org.talend.components -DartifactId=<nameOfTheNewTCOMP> -X

It will generate your archetype into your current folder.
\n Example : mvn archetype:generate -DarchetypeArtifactId=talendTcomp -DarchetypeGroupId=org.talend.components -DgroupId=org.talend.components.jms -DartifactId=jms -X