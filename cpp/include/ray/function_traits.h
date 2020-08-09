//
// Created by yu.qi on 2020/7/13.
//

#ifndef MYSO_FUNCTION_TRAITS_H
#define MYSO_FUNCTION_TRAITS_H

#include <type_traits>
#include "absl/meta/type_traits.h"
namespace ray {
    namespace api {
        template<typename T>
        struct function_traits;

        template<typename Ret, typename Arg, typename... Args>
        struct function_traits<Ret(Arg, Args...)> {
        public:
            enum {
                arity = sizeof...(Args) + 1
            };

            typedef Ret function_type(Arg, Args...);

            typedef Ret return_type;
            using stl_function_type = std::function<function_type>;

            typedef Ret(*pointer)(Arg, Args...);

            typedef std::tuple<Arg, Args...> tuple_type;
            typedef std::tuple <absl::remove_const_t<absl::remove_reference_t <
                                                     Arg>>, absl::remove_const_t <absl::remove_reference_t<Args>>...> bare_tuple_type;
            using args_tuple = std::tuple <std::string, Arg, absl::remove_const_t<absl::remove_reference_t < Args>>...>;
            using args_tuple_2nd = std::tuple <std::string, absl::remove_const_t<absl::remove_reference_t < Args>>...>;
        };

        template<typename Ret>
        struct function_traits<Ret()> {
        public:
            enum {
                arity = 0
            };

            typedef Ret function_type();

            typedef Ret return_type;
            using stl_function_type = std::function<function_type>;

            typedef Ret(*pointer)();

            typedef std::tuple<> tuple_type;
            typedef std::tuple<> bare_tuple_type;
            using args_tuple = std::tuple<std::string>;
            using args_tuple_2nd = std::tuple<std::string>;
        };

        template<typename Ret, typename... Args>
        struct function_traits<Ret(*)(Args...)> : function_traits<Ret(Args...)> {
        };

        template<typename Ret, typename... Args>
        struct function_traits<std::function < Ret(Args...)>> : function_traits<Ret(Args...)> {
    };

    template<typename ReturnType, typename ClassType, typename... Args>
    struct function_traits<ReturnType(ClassType::*)(Args...)> : function_traits<ReturnType(Args...)> {
    };

    template<typename ReturnType, typename ClassType, typename... Args>
    struct function_traits<ReturnType(ClassType::*)(Args...) const> : function_traits<ReturnType(Args...)> {
    };

    template<typename Callable>
    struct function_traits : function_traits<decltype(&Callable::operator())> {
    };
}
}
#endif //MYSO_FUNCTION_TRAITS_H
