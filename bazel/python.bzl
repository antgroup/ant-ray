# py_test_module_list creates a py_test target for each
# Python file in `files`
def py_test_module_list(files, size, deps, extra_srcs, name_suffix="", **kwargs):
    for file in files:
        if file in skiplist:
            continue
        # remove .py
        name = file[:-3] + name_suffix
        main = file
        native.py_test(
            name = name,
            size = size,
            main = file,
            srcs = extra_srcs + [file],
            **kwargs
        )

def py_test_run_all_subdirectory(include, exclude, extra_srcs, **kwargs):
    for file in native.glob(include = include, exclude = exclude):
        print(file)
        basename = file.rpartition("/")[-1]
        native.py_test(
            name = basename[:-3],
            srcs = extra_srcs + [file],
            **kwargs
        )


# === ANT-INTERNAL below ===

# Skip these test filess in internal code base.
# Because they require heavy depdencies.
# Note:
# 1) this only works for "bazel test", not "Pytest".
# 2) we can't skip them in "pytest_collection_modifyitems",
#    because that requires installing the dependencies as well.

skiplist = [
]
