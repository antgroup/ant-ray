# Note
pytest [resolve symlinks](https://github.com/pytest-dev/pytest/issues/5266) which will change module name from `ray.streaming.test` to `python.test`, Thus cause wrong ray function descriptor. So for test using pytest, we need to unlink symlink and copy file from `ray/streaming/python` to `ray/python/ray/streaming`:
```bash
rm -f python/ray/streaming && cp -rf streaming/python/ python/ray/streaming
```
see `buildtest.sh` streaming tests.

# New Python API
New Python API needs streaming-runtime jar and its dependency jars added to `java_worker_option`,  see `buildtest.sh`
## Test
* Build streaming jar. Execute `mvn -T10 package -DskipTests` for `streaming-runtime`, then
add path of `target/classes` and `target/lib` to `test_utils.py` to specify classpath. After this, you can run `python test_word_count.py`
```bash
echo "Build streaming jar"
cd ray/java
echo "Install ray java"
bazel build gen_maven_deps && mvn -T16 install -DskipTests -Dcheckstyle.skip
pip install -i https://pypi.antfin-inc.com/simple pyfury==0.0.6.4
pip install -i https://pypi.antfin-inc.com/simple zdfs-dfs-gcc492
cd -
pushd streaming/
output_log=1 sh build.sh compile
pushd runtime
bazel build gen_maven_deps && mvn -T16 install -DskipTests -Dcheckstyle.skip
export STREAMING_JAR="$PWD/target/classes:$PWD/target/test-classes:$PWD/target/lib/*"
echo "Streaming jar path $STREAMING_JAR"
popd
popd
```
if `STREAMING_JAR` is not provided, streaming integration test cases will be skipped
