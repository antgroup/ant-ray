workspace(name = "com_github_ray_project_ray")

local_repository(
    name = "com_alipay_ray_streaming",
    path = "../streaming",
)

local_repository(
    name = "com_alipay_ray_deploy",
    path = "../deploy",
)

load("//bazel:ray_deps_setup.bzl", "ray_deps_setup")

ray_deps_setup("../thirdparty")

load("//bazel:ray_deps_build_all.bzl", "ray_deps_build_all")

ray_deps_build_all()

load("//bazel:ray_extra_deps.bzl", "ray_extra_deps")

ray_extra_deps()
