
#include "plus.h"

int Return1() { return 1; };
int Plus1(int x) { return x + 1; };
int Plus(int x, int y) { return x + y; };
std::array<int, 100000> ReturnLargeArray(std::array<int, 100000> x) { return x; };
void ThrowTask() { throw std::logic_error("error"); }
std::string GetVal(ray::ObjectRef<std::string> obj) { return *obj.Get(); }
int Add(ray::ObjectRef<int> obj1, ray::ObjectRef<int> obj2) {
  return *obj1.Get() + *obj2.Get();
}

std::string Echo(std::string str) { return str; }

std::map<int, std::string> GetMap(std::map<int, std::string> map) {
  map.emplace(1, "world");
  return map;
}

std::array<std::string, 2> GetArray(std::array<std::string, 2> array) { return array; }

std::vector<std::string> GetList(std::vector<std::string> list) { return list; }

std::tuple<int, std::string> GetTuple(std::tuple<int, std::string> tp) { return tp; }

std::string GetNamespaceInTask() { return ray::GetNamespace(); }

Student GetStudent(Student student) { return student; }

std::map<int, Student> GetStudents(std::map<int, Student> students) { return students; }

RAY_REMOTE(Return1, Plus1, Plus, ThrowTask, ReturnLargeArray, Echo, GetMap, GetArray,
           GetList, GetTuple, GetNamespaceInTask, GetStudent, GetStudents);
