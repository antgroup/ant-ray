//
// Created by yu.qi on 2020/7/13.
//

#ifndef MYSO_MSGPACK_CODEC_H
#define MYSO_MSGPACK_CODEC_H

#include <msgpack.hpp>
namespace ray {
    namespace api {
        using buffer_type = msgpack::sbuffer;

        struct msgpack_codec {
            const static size_t init_size = 2 * 1024;

            template<typename... Args>
            static buffer_type pack_args(Args &&... args) {
              buffer_type buffer(init_size);
              msgpack::pack(buffer, std::forward_as_tuple(std::forward<Args>(args)...));
              return buffer;
            }

            template<typename Arg, typename... Args,
                    typename = typename std::enable_if<std::is_enum<Arg>::value>::type>
            static std::string pack_args_str(Arg arg, Args &&... args) {
              buffer_type buffer(init_size);
              msgpack::pack(buffer, std::forward_as_tuple((int) arg, std::forward<Args>(args)...));
              return std::string(buffer.data(), buffer.size());
            }

            template<typename T>
            buffer_type pack(T &&t) const {
              buffer_type buffer;
              msgpack::pack(buffer, std::forward<T>(t));
              return buffer;
            }

            template<typename T>
            T unpack(char const *data, size_t length) {
              try {
                msgpack::unpack(msg_, data, length);
                return msg_.get().as<T>();
              } catch (...) { throw std::invalid_argument("unpack failed: Args not match!"); }
            }

//            template<typename Result>
//            auto unpack0(char const *data, size_t length) {
//              try {
//                msgpack::unpack(msg_, data, length);
//                if constexpr (std::is_void_v < Result >) {
//                  return msg_.get().as < std::tuple < int >> ();
//                } else {
//                  return msg_.get().as < std::tuple < int, Result >> ();
//                }
//
//              } catch (...) { throw std::invalid_argument("unpack failed: arguments not match!"); }
//            }

        private:
            msgpack::unpacked msg_;
        };
    }
}
#endif //MYSO_MSGPACK_CODEC_H
