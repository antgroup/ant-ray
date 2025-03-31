#!/usr/bin/env bash
# Forked from X/ray/ci/lint/format.sh
# Black + Clang formatter (if installed). This script formats all changed files from the last mergebase.
# You are encouraged to run this locally before pushing changes for review.

# Cause the script to exit if a single command fails
set -euo pipefail

FLAKE8_VERSION_REQUIRED="3.9.1"
BLACK_VERSION_REQUIRED="21.12b0"
SHELLCHECK_VERSION_REQUIRED="0.7.1"
MYPY_VERSION_REQUIRED="0.782"

check_python_command_exist() {
    VERSION=""
    case "$1" in
        black)
            VERSION=$BLACK_VERSION_REQUIRED
            ;;
        flake8)
            VERSION=$FLAKE8_VERSION_REQUIRED
            ;;
        mypy)
            VERSION=$MYPY_VERSION_REQUIRED
            ;;
        *)
            echo "$1 is not a required dependency"
            exit 1
    esac
    if ! [ -x "$(command -v "$1")" ]; then
        echo "$1 not installed. Install the python package with: pip install $1==$VERSION"
        exit 1
    fi
}

check_python_command_exist black
check_python_command_exist flake8
check_python_command_exist mypy

# this stops git rev-parse from failing if we run this from the .git directory
builtin cd "$(dirname "${BASH_SOURCE:-$0}")"

ROOT="$(git rev-parse --show-toplevel)"
builtin cd "$ROOT" || exit 1

# ANT-INTERNAL: We only format "ray" sub-directory
builtin cd ray_common_libs/Ragent || exit 1

FLAKE8_VERSION=$(flake8 --version | head -n 1 | awk '{print $1}')
BLACK_VERSION=$(black --version | awk '{print $2}')
MYPY_VERSION=$(mypy --version | awk '{print $2}')

# params: tool name, tool version, required version
tool_version_check() {
    if [ "$2" != "$3" ]; then
        echo "WARNING: Ragent uses $1 $3, You currently are using $2. This might generate different results."
    fi
}

tool_version_check "flake8" "$FLAKE8_VERSION" "$FLAKE8_VERSION_REQUIRED"
tool_version_check "black" "$BLACK_VERSION" "$BLACK_VERSION_REQUIRED"
tool_version_check "mypy" "$MYPY_VERSION" "$MYPY_VERSION_REQUIRED"

if command -v shellcheck >/dev/null; then
    SHELLCHECK_VERSION=$(shellcheck --version | awk '/^version:/ {print $2}')
    tool_version_check "shellcheck" "$SHELLCHECK_VERSION" "$SHELLCHECK_VERSION_REQUIRED"
else
    echo "INFO: Ragent uses shellcheck for shell scripts, which is not installed. You may install shellcheck=$SHELLCHECK_VERSION_REQUIRED with your system package manager."
fi

if [[ $(flake8 --version) != *"flake8_quotes"* ]]; then
    echo "WARNING: Ragent uses flake8 with flake8_quotes. Might error without it. Install with: pip install flake8-quotes"
fi

if [[ $(flake8 --version) != *"flake8-bugbear"* ]]; then
    echo "WARNING: Ragent uses flake8 with flake8-bugbear. Might error without it. Install with: pip install flake8-bugbear"
fi

SHELLCHECK_FLAGS=(
  --exclude=1090  # "Can't follow non-constant source. Use a directive to specify location."
  --exclude=1091  # "Not following {file} due to some error"
  --exclude=2207  # "Prefer mapfile or read -a to split command output (or quote to avoid splitting)." -- these aren't compatible with macOS's old Bash
)

BLACK_EXCLUDES=(
    '--force-exclude' 'build/*'
)

GIT_LS_EXCLUDES=(
    ':(exclude).pytest_cache/'
)

# TODO(barakmich): This should be cleaned up. I've at least excised the copies
# of these arguments to this location, but the long-term answer is to actually
# make a flake8 config file
FLAKE8_PYX_IGNORES="--ignore=C408,E121,E123,E126,E211,E225,E226,E227,E24,E704,E999,W503,W504,W605"

shellcheck_scripts() {
  shellcheck "${SHELLCHECK_FLAGS[@]}" "$@"
}

# Format specified files
format_files() {
    local shell_files=() python_files=() bazel_files=()

    local name
    for name in "$@"; do
      local base="${name%.*}"
      local suffix="${name#${base}}"

      local shebang=""
      read -r shebang < "${name}" || true
      case "${shebang}" in
        '#!'*)
          shebang="${shebang#/usr/bin/env }"
          shebang="${shebang%% *}"
          shebang="${shebang##*/}"
          ;;
      esac

      if [ "${base}" = "WORKSPACE" ] || [ "${base}" = "BUILD" ] || [ "${suffix}" = ".BUILD" ] || [ "${suffix}" = ".bazel" ] || [ "${suffix}" = ".bzl" ]; then
        bazel_files+=("${name}")
      elif [ -z "${suffix}" ] && [ "${shebang}" != "${shebang#python}" ] || [ "${suffix}" != "${suffix#.py}" ]; then
        python_files+=("${name}")
      elif [ -z "${suffix}" ] && [ "${shebang}" != "${shebang%sh}" ] || [ "${suffix}" != "${suffix#.sh}" ]; then
        shell_files+=("${name}")
      else
        echo "error: failed to determine file type: ${name}" 1>&2
        return 1
      fi
    done

    if [ 0 -lt "${#python_files[@]}" ]; then
      black "${python_files[@]}"
    fi

    if command -v shellcheck >/dev/null; then
      if shellcheck --shell=sh --format=diff - < /dev/null; then
        if [ 0 -lt "${#shell_files[@]}" ]; then
          local difference
          difference="$(shellcheck_scripts --format=diff "${shell_files[@]}" || true && printf "-")"
          difference="${difference%-}"
          printf "%s" "${difference}" | patch -p1
        fi
      else
        echo "error: this version of shellcheck does not support diffs"
      fi
    fi
}

format_all_scripts() {
    command -v flake8 &> /dev/null;
    HAS_FLAKE8=$?

    echo "$(date)" "Black...."
    git ls-files -- '*.py' "${GIT_LS_EXCLUDES[@]}" | xargs -P 10 \
      black "${BLACK_EXCLUDES[@]}"
    echo "$(date)" "MYPY...."
    if [ $HAS_FLAKE8 ]; then
      echo "$(date)" "Flake8...."
      git ls-files -- '*.py' "${GIT_LS_EXCLUDES[@]}" | xargs -P 5 \
        flake8 --config=.flake8

      git ls-files -- '*.pyx' '*.pxd' '*.pxi' "${GIT_LS_EXCLUDES[@]}" | xargs -P 5 \
        flake8 --config=.flake8 "$FLAKE8_PYX_IGNORES"
    fi

    if command -v shellcheck >/dev/null; then
      local shell_files non_shell_files
      non_shell_files=($(git ls-files -- ':(exclude)*.sh'))
      shell_files=($(git ls-files -- '*.sh'))
      if [ 0 -lt "${#non_shell_files[@]}" ]; then
        shell_files+=($(git --no-pager grep -l -- '^#!\(/usr\)\?/bin/\(env \+\)\?\(ba\)\?sh' "${non_shell_files[@]}" || true))
      fi
      if [ 0 -lt "${#shell_files[@]}" ]; then
        echo "$(date)" "shellcheck scripts...."
        shellcheck_scripts "${shell_files[@]}"
      fi
    fi
}

# Format all files, and print the diff to stdout for travis.
# Mypy is run only on files specified in the array MYPY_FILES.
format_all() {
    format_all_scripts "${@}"

    echo "$(date)" "done!"
}

# Format files that differ from main branch. Ignores dirs that are not slated
# for autoformat yet.
format_changed() {
    # The `if` guard ensures that the list of filenames is not empty, which
    # could cause the formatter to receive 0 positional arguments, making
    # Black error.
    #
    # `diff-filter=ACRM` and $MERGEBASE is to ensure we only format files that
    # exist on both branches.

    # ANT-INTERNAL: we diff against origin/master of Ant internal repo.
    MERGEBASE="$(git merge-base origin/master HEAD)"

    # ANT-INTERNAL: we use "git diff --relative" instead of "git diff" to only format "ray" sub-directory.
    GIT_VERSION="$(git --version | cut -d' ' -f3)"
    GIT_VERSION_REQUIRED=2.28
    if [ "$(printf "%s\n%s" "$GIT_VERSION" "$GIT_VERSION_REQUIRED" | sort -V | head -n 1)" = "$GIT_VERSION" ]; then
      echo "ERROR: To find changed files in 'ray' sub-directory, Git version $GIT_VERSION_REQUIRED or above is required. Your current Git version is $GIT_VERSION. Please update Git and retry."
      exit 1
    fi
    git config diff.relative true
    # unset diff.relative when exiting the script
    trap 'git config --unset diff.relative' EXIT

    if ! git diff --diff-filter=ACRM --quiet --exit-code "$MERGEBASE" -- '*.py' &>/dev/null; then
        git diff --name-only --diff-filter=ACRM "$MERGEBASE" -- '*.py' | xargs -P 5 \
            black "${BLACK_EXCLUDES[@]}"
        if which flake8 >/dev/null; then
            git diff --name-only --diff-filter=ACRM "$MERGEBASE" -- '*.py' | xargs -P 5 \
                 flake8 --config=.flake8
        fi
    fi

    if ! git diff --diff-filter=ACRM --quiet --exit-code "$MERGEBASE" -- '*.pyx' '*.pxd' '*.pxi' &>/dev/null; then
        if which flake8 >/dev/null; then
            git diff --name-only --diff-filter=ACRM "$MERGEBASE" -- '*.pyx' '*.pxd' '*.pxi' | xargs -P 5 \
                 flake8 --config=.flake8 "$FLAKE8_PYX_IGNORES"
        fi
    fi

    if command -v shellcheck >/dev/null; then
        local shell_files non_shell_files
        non_shell_files=($(git diff --name-only --diff-filter=ACRM "$MERGEBASE" -- ':(exclude)*.sh'))
        shell_files=($(git diff --name-only --diff-filter=ACRM "$MERGEBASE" -- '*.sh'))
        if [ 0 -lt "${#non_shell_files[@]}" ]; then
            shell_files+=($(git --no-pager grep -l -- '^#!\(/usr\)\?/bin/\(env \+\)\?\(ba\)\?sh' "${non_shell_files[@]}" || true))
        fi
        if [ 0 -lt "${#shell_files[@]}" ]; then
            shellcheck_scripts "${shell_files[@]}"
        fi
    fi
}

# This flag formats individual files. --files *must* be the first command line
# arg to use this option.
if [ "${1-}" == '--files' ]; then
    format_files "${@:2}"
# If `--all` or `--scripts` are passed, then any further arguments are ignored.
# Format the entire python directory and other scripts.
elif [ "${1-}" == '--all-scripts' ]; then
    format_all_scripts "${@}"
    if [ -n "${FORMAT_SH_PRINT_DIFF-}" ]; then git --no-pager diff; fi
# Format the all Python, C++, Java and other script files.
elif [ "${1-}" == '--all' ]; then
    format_all "${@}"
    if [ -n "${FORMAT_SH_PRINT_DIFF-}" ]; then git --no-pager diff; fi
else
    # ANT-INTERNAL: we diff against origin/master of Ant internal repo.
    # # Add the upstream remote if it doesn't exist
    # if ! git remote -v | grep -q upstream; then
    #     git remote add 'upstream' 'https://github.com/ray-project/ray.git'
    # fi

    # Only fetch master since that's the branch we're diffing against.
    # ANT-INTERNAL: we diff against origin/master of Ant internal repo.
    git fetch origin master || true

    # Format only the files that changed in last commit.
    format_changed
fi

# Ensure import ordering
# Make sure that for every import psutil; import setproctitle
# There's a import ray above it.

PYTHON_EXECUTABLE=${PYTHON_EXECUTABLE:-python}

$PYTHON_EXECUTABLE check_import_order.py .

if ! git diff --quiet &>/dev/null; then
    echo 'Reformatted changed files. Please review and stage the changes.'
    echo 'Files updated:'
    echo

    git --no-pager diff --name-only

    exit 1
fi
