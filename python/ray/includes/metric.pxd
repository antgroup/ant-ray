from libcpp cimport bool as c_bool
from libcpp.string cimport string as c_string
from libcpp.unordered_map cimport unordered_map
from libcpp.vector cimport vector as c_vector
from libcpp.pair cimport pair as c_pair

cdef extern from "opencensus/tags/tag_key.h" nogil:
    cdef cppclass CTagKey "opencensus::tags::TagKey":
        @staticmethod
        CTagKey Register(c_string &name)
        const c_string &name() const

cdef extern from "ray/stats/metric.h" nogil:
    cdef cppclass CMetric "ray::stats::Metric":
        CMetric(const c_string &name,
                const c_string &description,
                const c_string &unit,
                const c_vector[CTagKey] &tag_keys)
        c_string GetName() const
        void Record(double value)
        void Record(double value,
                    unordered_map[c_string, c_string] &tags)

    cdef cppclass CGauge "ray::stats::Gauge":
        CGauge(const c_string &name,
               const c_string &description,
               const c_string &unit,
               const c_vector[CTagKey] &tag_keys)

    cdef cppclass CCount "ray::stats::Count":
        CCount(const c_string &name,
               const c_string &description,
               const c_string &unit,
               const c_vector[CTagKey] &tag_keys)

    cdef cppclass CSum "ray::stats::Sum":
        CSum(const c_string &name,
             const c_string &description,
             const c_string &unit,
             const c_vector[CTagKey] &tag_keys)

    cdef cppclass CHistogram "ray::stats::Histogram":
        CHistogram(const c_string &name,
                   const c_string &description,
                   const c_string &unit,
                   const c_vector[double] &boundaries,
                   const c_vector[CTagKey] &tag_keys)

cdef extern from "<utility>" namespace "std" nogil:
    cdef c_pair[T, U] make_pair[T, U](T&, U&)

cdef extern from "ray/stats/stats.h" namespace "ray::stats" nogil:
    cdef void Start(const c_vector[c_pair[CTagKey, c_string]] &global_tags,
                    const int metrics_agent_port,
                    const c_string &config_list,
                    const c_bool callback_shutdown,
                    const c_bool init_log,
                    const c_string &app_name)

    cdef void Shutdown()
