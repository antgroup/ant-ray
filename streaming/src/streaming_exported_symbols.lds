# This file defines the C++ symbols that need to be exported (aka ABI, application binary interface).
# These symbols will be used by other libraries (e.g., streaming).
# Note: This file is used for macOS only, and should be kept in sync with `ray_version_script.lds`.
# Ray ABI is not finalized, the exact set of exported (C/C++) APIs is subject to change.
# common
*ray*streaming*
# util
# id
# Others