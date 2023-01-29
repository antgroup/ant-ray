#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e
# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

jar_path="$ROOT_DIR/../bazel-bin/java"
ray_java_classes="/tmp/ray_java_classes"
ray_java_src="/tmp/ray_java_src"
ray_coverage_java_report=$1
ray_coverage_xml="${ray_coverage_java_report}/cobertura.xml"

extract_classes() {
    set -e
    rm -rf ${ray_java_classes}
    mkdir ${ray_java_classes}
    # shellcheck disable=SC2010
    ray_jars=$(ls "${jar_path}" | grep -P '(^libio_ray_ray_)([a-z]*)(\.jar$)')
    for jar in ${ray_jars}; do
        #When the command fails, do not quit
        set +e

        jar_name=$(echo "${jar}" | awk -F . '{print $1}')
        tmp_jar_path=${ray_java_classes}/${jar_name}

        #Remove test class
        val=$(echo "${jar}" | grep "test")
        if [  ${#val} -ne 0 ];then
            continue
        fi

        mkdir "${tmp_jar_path}"
        cp "${jar_path}/${jar}" "${tmp_jar_path}"
        pushd "${tmp_jar_path}"
        #decompress yar jar
        jar xvf "${tmp_jar_path}/${jar}"
        #delete jar,otherwise will wrong coverage data
        rm "${tmp_jar_path}/${jar}"
        #delete generated directory
        dirsname=$(find "${tmp_jar_path}"  -name generated)
        for dir in ${dirsname};do
            if [ -d "${dir}" ]; then
                rm -rf "${dir}"
            fi
        done
        popd
    done
}

extract_java(){
    rm -rf ${ray_java_src}
    mkdir ${ray_java_src}
    # shellcheck disable=SC2010
    ray_src_jars=$(ls "${jar_path}" | grep -P '(^libio_ray_ray_)([a-z]*)(-src)(\.jar$)')
    for src_jar in ${ray_src_jars}; do
        #When the command fails, do not quit
        set +e

        #Remove test class
        val=$(echo "${src_jar}" | grep "test")
        if [  ${#val} -ne 0 ];then
            continue
        fi

        cp "${jar_path}/${src_jar}" "${ray_java_src}"
        pushd "${ray_java_src}"
        #decompress yar jar
        jar xvf "${ray_java_src}/${src_jar}"
        popd
    done
}

report(){
    rm -rf "${ray_coverage_java_report}"
    mkdir "${ray_coverage_java_report}"
    pushd "${ray_coverage_java_report}"

    #generate coverage report cobertura.xml
    java -jar "$ROOT_DIR/jacococli.jar" report  /tmp/jacoco_cluster.exec --classfiles "${ray_java_classes}"  --xml "${ray_coverage_xml}" --sourcefiles "${ray_java_src}"
    #check cobertura.xml
    ls -al "$ray_coverage_java_report"
    popd
}

set_down() {
    rm -rf ${ray_java_classes} ${ray_java_src} /tmp/jacoco_cluster.exec /tmp/jacoco_single.exec
}

#step1. extract classes files from ray jar to tmp directory
extract_classes
#step2. extract java files from ray jar to tmp directory
extract_java

#step3. generate java coverage report
report
#step4. remove tmp files and directory
set_down

