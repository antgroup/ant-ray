#!/bin/bash
# shellcheck disable=SC2086
# This script is for users to build docker images locally. It is most useful for users wishing to edit the
# base-deps, or ray images. This script is *not* tested.

GPU=""
BASE_IMAGE="ubuntu:22.04"
PYTHON_VERSION="3.9"

BUILD_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --gpu)
            GPU="-gpu"
            BASE_IMAGE="nvidia/cuda:11.8.0-cudnn8-devel-ubuntu22.04"
        ;;
        --base-image)
            # Override for the base image.
            shift
            BASE_IMAGE="$1"
        ;;
        --no-cache-build)
            BUILD_ARGS+=("--no-cache")
        ;;
        --shas-only)
            # output the SHA sum of each build. This is useful for scripting tests,
            # especially when builds of different versions are running on the same machine.
            # It also can facilitate cleanup.
            OUTPUT_SHA=YES
            BUILD_ARGS+=("-q")
        ;;
        --python-version)
            # Python version to install. e.g. 3.9
            # Changing python versions may require a different wheel.
            # If not provided defaults to 3.9
            shift
            PYTHON_VERSION="$1"
        ;;
        *)
            echo "Usage: build-docker.sh [ --gpu ] [ --base-image ] [ --no-cache-build ] [ --shas-only ] [ --build-development-image ] [ --build-examples ] [ --python-version ]"
            exit 1
    esac
    shift
done

export DOCKER_BUILDKIT=1

# Build base-deps image
if [[ "$OUTPUT_SHA" != "YES" ]]; then
    echo "=== Building base-deps image ===" >/dev/stderr
fi

BUILD_CMD=(
    docker build "${BUILD_ARGS[@]}"
    --build-arg BASE_IMAG="$BASE_IMAGE"
    --build-arg PYTHON_VERSION="${PYTHON_VERSION}"
    -t "antgroup/base-deps:dev$GPU" -f "docker/base-deps/Dockerfile" .
)

if [[ "$OUTPUT_SHA" == "YES" ]]; then
    IMAGE_SHA="$("${BUILD_CMD[@]}")"
    echo "antgroup/base-deps:dev$GPU SHA:$IMAGE_SHA"
else
    "${BUILD_CMD[@]}"
fi

# Build ray image
if [[ "$OUTPUT_SHA" != "YES" ]]; then
    echo "=== Building ray image ===" >/dev/stderr
fi

RAY_BUILD_DIR="$(mktemp -d)"
mkdir -p "$RAY_BUILD_DIR/.whl"
chmod 777 "$RAY_BUILD_DIR/.whl"

docker run --rm \
    -v "$RAY_BUILD_DIR/.whl":/wheels \
    --user "$(id -u):$(id -g)" \
    antgroup/base-deps:dev$GPU \
    pip download --no-cache-dir --quiet --no-deps --only-binary=:all: ant-ray -d /wheels

WHEEL_FILE=$(ls "$RAY_BUILD_DIR"/.whl/*.whl 2>/dev/null | head -n1)
if [[ -z "$WHEEL_FILE" ]]; then
    echo "Error: No wheel downloaded for ant-ray" >&2
    exit 1
fi
WHEEL="$(basename "$WHEEL_DIR"/.whl/ant*.whl)"


cp python/requirements_compiled.txt "$RAY_BUILD_DIR"
cp docker/ray/Dockerfile "$RAY_BUILD_DIR"

BUILD_CMD=(
    docker build "${BUILD_ARGS[@]}"
    --build-arg FULL_BASE_IMAGE="antgroup/base-deps:dev$GPU"
    --build-arg WHEEL_PATH=".whl/${WHEEL}"
    -t "antgroup/ant-ray:dev$GPU" "$RAY_BUILD_DIR"
)

if [[ "$OUTPUT_SHA" == "YES" ]]; then
    IMAGE_SHA="$("${BUILD_CMD[@]}")"
    echo "antgroup/ant-ray:dev$GPU SHA:$IMAGE_SHA"
else
    "${BUILD_CMD[@]}"
fi

rm -rf "$RAY_BUILD_DIR"