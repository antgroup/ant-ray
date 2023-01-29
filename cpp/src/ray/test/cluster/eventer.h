
#pragma once

#include <ray/api.h>

/// class used to test event interface.
class Eventer {
 public:
  Eventer();
  static Eventer *FactoryCreate();

  bool WriteEvent();
  bool WriteEventInPrivateThread();
  int SearchEvent(const std::string &path, const std::string &label,
                  const std::string &msg);
};

int Search(const std::string &path, const std::string &label, const std::string &msg);