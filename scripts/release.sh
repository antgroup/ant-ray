#!/usr/bin/env bash
# The relationships between develop and release branch are illustrated below:
#
# x.x.x branch (tagged by ant-ray-x.x.x and passed all release tests)
#        -----+---*---------------------------------------
#                    \
#        (new branch) \ commits (change version)
#                      \   v
# release-ant-ray-x.x.x branch (used for version bumping only)
#        ------------------+-------------*----------------
#                                        v
#                   Push to remote repo and run release pipeline

set -eu

RELEASE_VERSION="$1"
PYTHON_OLD_VERSION=$(python -c "import python.ray.__init__ as ray; print(ray.__version__)")

# make ensure in the right branch
echo -e "You are currently in branch \033[31m$(git branch --show-current)\033[0m and"
echo -e "ready to release \033[31mant-ray-$RELEASE_VERSION\033[0m"
read -r -p "Please confirm branch name and version. yes or no: " reply
if [ "$reply" != "yes" ]
then
    echo "aborted by user"
    exit 1
fi

# git tag and push signal tag to remote.
# If the tag already exists, the script will exit.
git tag -a "ant-ray-${RELEASE_VERSION}" -m "v${RELEASE_VERSION}"
git push origin "ant-ray-${RELEASE_VERSION}"

# checkout to release branch
# If branch release already exists, the script will exit.
git checkout -b "release-ant-ray-$RELEASE_VERSION"

case "$#" in
  1)
    JAVA_OLD_VERSION="1.0.0-SNAPSHOT"
    ;;
  2)
    JAVA_OLD_VERSION="$2"
    ;;
  *)
    echo "wrong args number: $#"
    exit
esac

# Determine the operating system.
unameOut="$(uname -s)"
case "${unameOut}" in
    Linux*)     machine=Linux;;
    Darwin*)    machine=Mac;;
    CYGWIN*)    machine=Cygwin;;
    MINGW*)     machine=MinGw;;
    *)          machine="UNKNOWN:${unameOut}"
esac

echo "begin release ray, version: $RELEASE_VERSION"
echo "old python version: $PYTHON_OLD_VERSION, old java version: $JAVA_OLD_VERSION"
echo "run in: ${machine} ..."

JAVA_VERSION_PATHS=$(git grep -l "$JAVA_OLD_VERSION" | tr "\n" " ")
PYTHON_VERSION_PATH="python/ray/__init__.py"

# Run release script in right place.
if [ ! -f "$PYTHON_VERSION_PATH" ]; then
    echo "Please in dir \$X_HOME/ray, then run script: scripts/release.sh"
    exit 1
fi


case "$machine" in
  "Linux")
    sed -i s/"__version__ = \"$PYTHON_OLD_VERSION\"/__version__ = \"$RELEASE_VERSION\""/ "$PYTHON_VERSION_PATH"
    for JAVA_PATH in $JAVA_VERSION_PATHS
    do
        if  [[ "$JAVA_PATH" == java/* ]] ;
        then
            sed -i "s/$JAVA_OLD_VERSION/$RELEASE_VERSION/g" "$JAVA_PATH"
        fi
    done
    ;;
  "Mac")
    sed -i "" -e s/"__version__ = \"$PYTHON_OLD_VERSION\"/__version__ = \"$RELEASE_VERSION\""/ "$PYTHON_VERSION_PATH"
    for JAVA_PATH in $JAVA_VERSION_PATHS
    do
        if  [[ "$JAVA_PATH" == java/* ]] ;
        then
            sed -i "" -e "s/$JAVA_OLD_VERSION/$RELEASE_VERSION/g" "$JAVA_PATH"
        fi
    done
    ;;
  *)
    echo "unknown operating system: $machine"
    exit 1
esac

git -c color.ui=always status
git diff --color "$PYTHON_VERSION_PATH" | cat
for JAVA_PATH in $JAVA_VERSION_PATHS
do
    if  [[ "$JAVA_PATH" == java/* ]] ;
    then
        git diff --color "$JAVA_PATH" | cat
    fi
done

# check by user
read -r -p "Push version $RELEASE_VERSION to code.alipay.com/Arc/X yes or no: " reply
if [ "$reply" != "yes" ]
then
    echo "aborted by user"
    exit 1
fi

# git add
git add "$PYTHON_VERSION_PATH"
for JAVA_PATH in $JAVA_VERSION_PATHS
do
    if  [[ "$JAVA_PATH" == java/* ]] ;
    then
        git add "$JAVA_PATH"
    fi
done

# git commit
git commit -m "bump version to $RELEASE_VERSION"

GIT_CURRENT_BRANCH=$(git branch --show-current)

# git tag and push signal tag to remote.
git push origin "$GIT_CURRENT_BRANCH"

echo "success release ray to version: $RELEASE_VERSION"
echo -e "Please run release pipeline on branch \033[31m$GIT_CURRENT_BRANCH\033[0m."