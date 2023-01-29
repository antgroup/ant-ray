
#pragma once

#include <ray/api.h>

/// general function of user code
int Return1();
int Plus1(int x);
int Plus(int x, int y);
void ThrowTask();

std::array<int, 100000> ReturnLargeArray(std::array<int, 100000> x);
std::string Echo(std::string str);
std::map<int, std::string> GetMap(std::map<int, std::string> map);
std::array<std::string, 2> GetArray(std::array<std::string, 2> array);
std::vector<std::string> GetList(std::vector<std::string> list);
std::tuple<int, std::string> GetTuple(std::tuple<int, std::string> tp);

std::string GetNamespaceInTask();

struct Student {
  std::string name;
  int age;
  MSGPACK_DEFINE(name, age);
};

Student GetStudent(Student student);
std::map<int, Student> GetStudents(std::map<int, Student> students);
