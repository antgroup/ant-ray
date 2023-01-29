def test_import_ray_streaming():
    try:
        import ray.streaming  # noqa: F401
    except Exception:
        assert False
