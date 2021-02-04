

@ray.remote
class A:
    pass

@ray.remote
class B:
    def __init__(self, handle):
        self.handle = handle

a = A.options(name="a").remote()
b = B.options(name="b").remote(a)

