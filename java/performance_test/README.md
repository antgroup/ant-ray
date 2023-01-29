1. 编译ray
cd ray
pip install -e ./python/

cd ray/java/
mvn clean install -DskipTests -T4  -Dcheckstyle.skip

cd ray/java/performance_test
// pom.xml文件改过上次被回退了，还没修，先checkout
git checkout 6dc7091af71cc0b31a46c686f0600ad3728d6f22 pom.xml
mvn clean install -DskipTests -T4  -Dcheckstyle.skip

// 跑test
PERF_TEST_BATCH_SIZE=1000 PERF_TEST_DEV=true java -DMyDriver -cp ./target/ray-performance-test-1.0.0-SNAPSHOT-jar-with-dependencies.jar:../../build/java/*  io.ray.performancetest.test.ActorPerformanceTestCase1

