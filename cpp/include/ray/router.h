//
// Created by yu.qi on 2020/7/21.
//

#ifndef RAY_ROUTER_H
#define RAY_ROUTER_H
#include <string>
#include <unordered_map>
#include "function_traits.h"
#include "msgpack_codec.h"

namespace ray {
namespace api {
    enum  result_code {
        OK = 0,
        FAIL = 1,
    };
    static const size_t MAX_BUF_LEN = 1048576 * 10;

    class router {
    public:
        static router& get() {
          static router instance;
          return instance;
        }

        template<typename Function>
        void register_handler(std::string const& name, Function f) {
          return register_nonmember_func(name, std::move(f));
        }

        template<typename Function, typename Self>
        void register_handler(std::string const& name, const Function& f, Self* self) {
          return register_member_func(name, f, self);
        }

        void remove_handler(std::string const& name) { this->map_invokers_.erase(name); }

        std::string route(const char* data, std::size_t size) {
          std::string result;
          do{
            try {
              msgpack_codec codec;
              auto p = codec.unpack<std::tuple<std::string>>(data, size);
              auto& func_name = std::get<0>(p);
              auto it = map_invokers_.find(func_name);
              if (it == map_invokers_.end()) {
                result = codec.pack_args_str(result_code::FAIL, "unknown function: " + func_name);
                break;
              }

              it->second(data, size, result);
              if (result.size() >= MAX_BUF_LEN) {
                result = codec.pack_args_str(result_code::FAIL, "the response result is out of range: more than 10M " + func_name);
              }
            }
            catch (const std::exception & ex) {
              msgpack_codec codec;
              result = codec.pack_args_str(result_code::FAIL, ex.what());
            }
          }while(0);

          return result;
        }

        router() = default;

    private:
        router(const router&) = delete;
        router(router&&) = delete;

        template<typename F, size_t... I, typename Arg, typename... Args>
        static typename std::result_of<F(Args...)>::type call_helper(
                const F & f, const absl::index_sequence<I...>&, std::tuple<Arg, Args...> tup) {
          return f(std::move(std::get<I + 1>(tup))...);
        }

        template<typename F, typename Arg, typename... Args>
        static
        typename std::enable_if<std::is_void<typename std::result_of<F(Args...)>::type>::value>::type
        call(const F & f, std::string & result, std::tuple<Arg, Args...> tp) {
          call_helper(f, absl::make_index_sequence<sizeof...(Args)>{}, std::move(tp));
          result = msgpack_codec::pack_args_str(result_code::OK);
        }

        template<typename F, typename Arg, typename... Args>
        static
        typename std::enable_if<!std::is_void<typename std::result_of<F(Args...)>::type>::value>::type
        call(const F & f, std::string & result, std::tuple<Arg, Args...> tp) {
          auto r = call_helper(f, absl::make_index_sequence<sizeof...(Args)>{}, std::move(tp));
          msgpack_codec codec;
          result = msgpack_codec::pack_args_str(result_code::OK, r);
        }

        template<typename F, typename Self, size_t... Indexes, typename Arg, typename... Args>
        static typename std::result_of<F(Self, Args...)>::type call_member_helper(
                const F & f, Self * self, const absl::index_sequence<Indexes...>&,
                std::tuple<Arg, Args...> tup) {
          return (*self.*f)(std::move(std::get<Indexes + 1>(tup))...);
        }

        template<typename F, typename Self, typename Arg, typename... Args>
        static typename std::enable_if<
                std::is_void<typename std::result_of<F(Self, Args...)>::type>::value>::type
        call_member(const F & f, Self * self, std::string & result,
                    std::tuple<Arg, Args...> tp) {
          call_member_helper(f, self, typename absl::make_index_sequence<sizeof...(Args)>{}, std::move(tp));
          result = msgpack_codec::pack_args_str(result_code::OK);
        }

        template<typename F, typename Self, typename Arg, typename... Args>
        static typename std::enable_if<
                !std::is_void<typename std::result_of<F(Self, Args...)>::type>::value>::type
        call_member(const F & f, Self * self, std::string & result,
                    std::tuple<Arg, Args...> tp) {
          auto r =
                  call_member_helper(f, self, typename absl::make_index_sequence<sizeof...(Args)>{}, std::move(tp));
          result = msgpack_codec::pack_args_str(result_code::OK, r);
        }

        template<typename Function>
        struct invoker {
            static inline void apply(const Function& func, const char* data, size_t size,
                                     std::string& result) {
              using args_tuple = typename function_traits<Function>::args_tuple;
              msgpack_codec codec;
              try {
                auto tp = codec.unpack<args_tuple>(data, size);
                router::call(func, result, std::move(tp));
              }
              catch (std::invalid_argument & e) {
                result = codec.pack_args_str(result_code::FAIL, e.what());
              }
              catch (const std::exception & e) {
                result = codec.pack_args_str(result_code::FAIL, e.what());
              }
            }

            template<typename Self>
            static inline void apply_member(const Function& func, Self* self,
                                            const char* data, size_t size, std::string& result) {
              using args_tuple = typename function_traits<Function>::args_tuple;
              msgpack_codec codec;
              try {
                auto tp = codec.unpack<args_tuple>(data, size);
                call_member(func, self, result, std::move(tp));
              }
              catch (std::invalid_argument & e) {
                result = codec.pack_args_str(result_code::FAIL, e.what());
              }
              catch (const std::exception & e) {
                result = codec.pack_args_str(result_code::FAIL, e.what());
              }
            }
        };

        template<typename Function>
        void register_nonmember_func(std::string const& name, Function f) {
          this->map_invokers_[name] = { std::bind(&invoker<Function>::apply, std::move(f), std::placeholders::_1,
                                                  std::placeholders::_2, std::placeholders::_3) };
        }

        template<typename Function, typename Self>
        void register_member_func(const std::string& name, const Function& f, Self* self) {
          this->map_invokers_[name] = { std::bind(&invoker<Function>::template apply_member<Self>,
                                                  f, self, std::placeholders::_1, std::placeholders::_2,
                                                  std::placeholders::_3) };
        }

        std::unordered_map<std::string,
        std::function<void(const char*, size_t, std::string&)>>
                map_invokers_;
    };
}
}
#endif //RAY_ROUTER_H
