load("@rules_jvm_external//:defs.bzl", "maven_install")
load("@rules_jvm_external//:specs.bzl", "maven")

def gen_streaming_java_deps():
    maven_install(
        name = "ray_streaming_maven",
        artifacts = [
            "com.google.guava:guava:20.0",
            "com.google.code.findbugs:jsr305:3.0.2",
            "com.google.code.gson:gson:2.8.5",
            "com.github.davidmoten:flatbuffers-java:1.9.0.1",
            "com.google.protobuf:protobuf-java:3.8.0",
            "org.apache.commons:commons-lang3:3.5",
            "de.ruedigermoeller:fst:2.57.3-SNAPSHOT",
            "org.aeonbits.owner:owner:1.0.10",
            "org.slf4j:slf4j-api:1.7.12",
            "org.slf4j:slf4j-log4j12:1.7.25",
            "io.ant-ray:state:2.6.7-SNAPSHOT",
            "org.apache.logging.log4j:log4j-core:2.14.0",
            "org.testng:testng:7.3.0",
            "log4j:log4j:1.2.17",
            "org.mockito:mockito-all:1.10.19",
            "org.apache.commons:commons-lang3:3.3.2",
            "org.msgpack:msgpack-core:0.8.20",
            "org.testng:testng:7.3.0",
	        "org.mockito:mockito-all:1.10.19",
	        "org.powermock:powermock-module-testng:1.6.6",
	        "org.powermock:powermock-api-mockito:1.6.6",
	        "io.netty:netty-codec-http2:4.1.25.Final",
	        "com.esotericsoftware:kryo:4.0.0",
	        "de.javakaffee:kryo-serializers:0.42",
	        "com.esotericsoftware.minlog:minlog:1.2",
	        "com.esotericsoftware.reflectasm:reflectasm:1.07",
	        "com.typesafe:config:1.3.2",
	        "org.objenesis:objenesis:2.4",
	        "com.google.guava:guava:20.0",
            "org.ow2.asm:asm:6.0",
	        "org.projectlombok:lombok:1.14.8",
            maven.artifact(
                group = "io.ant-ray",
                artifact = "fury",
                version = "0.6.4",
                exclusions = [
                    "io.netty:netty-buffer",
                    "io.netty:netty-common",
                ]
            ),
            maven.artifact(
                group = "org.apache.arrow",
                artifact = "arrow-vector",
                version = "1.0.0",
                exclusions = [
                    "io.netty:netty-buffer",
                    "io.netty:netty-common",
                ]
            ),
        ],
        repositories = [
            "http://mvn.dev.alipay.net:8080/artifactory/repo/",
        ],
        fail_on_missing_checksum = False,
    )
