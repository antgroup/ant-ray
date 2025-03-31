import ray
from pydantic import BaseModel


class My(BaseModel):
    def func(self, param: BaseModel):
        self._param = param
        return param


class B(BaseModel):
    a: int


my = ray.remote(My).remote()
ref = my.func.remote(B(a=1))
print(ray.get(ref))
