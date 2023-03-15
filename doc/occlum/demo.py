import ray

print("Calling ray.init")
ray.init(
    object_store_memory=100 * 1024 * 1024,
    _temp_dir="/host/tmp/ray",
    _plasma_directory="/tmp"
)

@ray.remote
def foo(x):
    return ray.put(f"Result data is: {x + 1}")

@ray.remote
class Owner:
    def warmup(self):
        return ray.get(ray.put("warmup"))

@ray.remote
class Borrower:
    def borrow(self, obj_ref):
        self.obj = ray.get(obj_ref)
        return self.obj

print("Putting & getting value to/from object store")
obj_ref = ray.put("123")
value = ray.get(obj_ref)
assert value == "123"

print("Calling ray.get")
value = ray.get(ray.get(foo.remote(1)))
print(f"Getting value from `ray.get`: {value}")
assert value == "Result data is: 2"

print("Starting `Owner` Actor")
owner = Owner.remote()
obj_ref = owner.warmup.remote()
obj = ray.get(obj_ref)
print(f"Getting value from Actor task `warmup`: {obj}")

print("Starting `Owner` Actor")
borrower = Borrower.remote()
obj = ray.get(borrower.borrow.remote(obj_ref))
print(f"Getting value from Actor task `borrow`: {obj}")
