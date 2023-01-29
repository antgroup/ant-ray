import ray


@ray.remote
def f():
    return 100


def main():
    # Note that the job_id here should be
    job_id = ray.JobID.from_int(1)
    ray.init(job_id=job_id, redis_address="127.0.0.1:34222")

    obj = f.remote()
    return ray.get(obj)
