load("@rules_jvm_external//:defs.bzl", "maven_install")
load("@rules_jvm_external//:specs.bzl", "maven")

def gen_java_deps():
    maven_install(
        name = "maven",
        artifacts = [
            "com.google.code.gson:gson:2.8.5",
            "com.google.guava:guava:30.0-jre",
            "com.google.protobuf:protobuf-java:3.16.0",
            "com.puppycrawl.tools:checkstyle:8.15",
            "com.sun.xml.bind:jaxb-core:2.3.0",
            "com.sun.xml.bind:jaxb-impl:2.3.0",
            "com.taobao.monitor:kmonitor-client:1.1.4",
            "com.typesafe:config:1.3.2",
            "commons-io:commons-io:2.7",
            "javax.xml.bind:jaxb-api:2.3.0",
           "javax.activation:activation:1.1.1",
            "org.apache.commons:commons-lang3:3.4",
            "org.msgpack:msgpack-core:0.8.20",
            "org.ow2.asm:asm:6.0",
            "org.apache.logging.log4j:log4j-api:2.14.0",
            "org.apache.logging.log4j:log4j-core:2.14.0_message_nolookup",
            "org.apache.logging.log4j:log4j-slf4j-impl:2.14.0",
            "org.slf4j:slf4j-api:1.7.25",
            "com.lmax:disruptor:3.3.4",
            "com.lmax:disruptor:3.3.4",
            "org.yaml:snakeyaml:1.26",
            "redis.clients:jedis:2.8.0",
            "commons-codec:commons-codec:1.9",
            "org.mockito:mockito-core:2.23.0",
            "org.powermock:powermock-core:2.0.2",
            "org.powermock:powermock-reflect:2.0.2",
            "org.powermock:powermock-api-mockito2:2.0.2",
            "org.powermock:powermock-module-testng:2.0.2",
            "net.java.dev.jna:jna:5.5.0",
            "de.ruedigermoeller:fst:2.57.3-SNAPSHOT",
            maven.artifact(
                group = "org.apache.httpcomponents.client5",
                artifact = "httpclient5",
                version = "5.0.3",
                exclusions = [
                    "commons-codec:commons-codec",
                ]
            ),
            "org.apache.httpcomponents.core5:httpcore5:5.0.2",
            maven.artifact(
                group = "org.testng",
                artifact = "testng",
                version = "7.3.0",
                exclusions = [
                    "org.yaml:snakeyaml",
                ],
            ),
        ],
        repositories = [
            "http://mvn.dev.alipay.net:8080/artifactory/repo/",
            "http://mvn.test.alipay.net:8080/artifactory/repo/",
        ],
    )
