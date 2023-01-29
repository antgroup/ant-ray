#include <Python.h>
#include <unistd.h>

#include <cstdlib>
#include <map>
#include <string>

#include "metrics/kmonitor_reporter.h"
#include "streaming/src/util/logging.h"

typedef struct {
  PyObject_HEAD std::shared_ptr<ray::streaming::StreamingKmonitorClient> kmonitor_client =
      nullptr;
} PyStreamingKmonitor;

int convert2map(PyObject *dict, std::map<std::string, std::string> &tags) {
  if (!PyDict_Check(dict)) {
    return 0;
  }
  PyObject *key;
  PyObject *value;
  Py_ssize_t i;
  i = 0;
  while (PyDict_Next(dict, &i, &key, &value)) {
    std::string key_str(PyUnicode_AsUTF8(key));
    std::string value_str(PyUnicode_AsUTF8(value));
    tags[key_str] = value_str;
  }
  return 1;
}

static PyObject *kmonitor_test(PyObject *self, PyObject *args) {
  return (PyObject *)Py_BuildValue("l", 1);
}

static PyMethodDef streaming_kmonitor_methods[] = {
    {"test", kmonitor_test, METH_VARARGS, "test"}, {NULL, NULL, 0, NULL} /* Sentinel */
};

static PyObject *PyStreamingKmonitor_start(PyStreamingKmonitor *self, PyObject *args,
                                           PyObject *kwds) {
  char *json_str = nullptr;
  int json_str_size = 0;
  if (!PyArg_ParseTuple(args, "s#", &json_str, &json_str_size)) {
    PyErr_SetString(PyExc_TypeError, "arg parse error");
    return NULL;
  }
  if (nullptr == json_str) {
    PyErr_SetString(PyExc_ValueError, "config can't be empty");
    return NULL;
  }
  std::string json_string(json_str);
  STREAMING_LOG(INFO) << "kmonitor init json string => " << json_string;
  self->kmonitor_client = std::make_shared<ray::streaming::StreamingKmonitorClient>();
  self->kmonitor_client->Start(json_string);
  STREAMING_LOG(INFO) << "kmonitor client start";

  return (PyObject *)Py_BuildValue("s", "start");
}

static PyObject *PyStreamingKmonitor_stop(PyStreamingKmonitor *self, PyObject *args,
                                          PyObject *kwds) {
  self->kmonitor_client->Shutdown();
  STREAMING_LOG(INFO) << "kmonitor client stop";
  return (PyObject *)Py_BuildValue("s", "stop");
}

static PyObject *PyStreamingKmonitor_update_gauge(PyStreamingKmonitor *self,
                                                  PyObject *args, PyObject *kwds) {
  char *metric_name = nullptr;
  double value = 0.0;
  std::map<std::string, std::string> tags;
  if (!PyArg_ParseTuple(args, "sd|O&", &metric_name, &value, &convert2map, &tags)) {
    PyErr_SetString(PyExc_TypeError, "arg parse error");
    return NULL;
  }
  // It will coredump to append null string
  if (nullptr == metric_name) {
    PyErr_SetString(PyExc_ValueError, "metric name can't be empty");
    return NULL;
  }
  std::string metric_str(metric_name);
  self->kmonitor_client->UpdateGauge(metric_str, tags, value);
  STREAMING_LOG(DEBUG) << "kmonitor report GAUGE => " << metric_str << " value => "
                       << value;
  return (PyObject *)Py_BuildValue("s", "gauge");
}

static PyObject *PyStreamingKmonitor_update_counter(PyStreamingKmonitor *self,
                                                    PyObject *args, PyObject *kwds) {
  char *metric_name = nullptr;
  double value = 0.0;
  std::map<std::string, std::string> tags;
  if (!PyArg_ParseTuple(args, "sd|O&", &metric_name, &value, &convert2map, &tags)) {
    PyErr_SetString(PyExc_TypeError, "arg parse error");
    return NULL;
  }
  if (nullptr == metric_name) {
    PyErr_SetString(PyExc_ValueError, "metric name can't be empty");
    return NULL;
  }
  std::string metric_str(metric_name);
  self->kmonitor_client->UpdateCounter(metric_str, tags, value);
  STREAMING_LOG(DEBUG) << "kmonitor report COUNTER => " << metric_str << " value => "
                       << value;
  return (PyObject *)Py_BuildValue("s", "counter");
}

static PyObject *PyStreamingKmonitor_update_qps(PyStreamingKmonitor *self, PyObject *args,
                                                PyObject *kwds) {
  char *metric_name = nullptr;
  double value = 0.0;
  std::map<std::string, std::string> tags;
  if (!PyArg_ParseTuple(args, "sd|O&", &metric_name, &value, &convert2map, &tags)) {
    PyErr_SetString(PyExc_TypeError, "arg parse error");
    return NULL;
  }
  if (nullptr == metric_name) {
    PyErr_SetString(PyExc_ValueError, "metric name can't be empty");
    return NULL;
  }
  std::string metric_str(metric_name);
  self->kmonitor_client->UpdateQPS(metric_str, tags, value);
  STREAMING_LOG(DEBUG) << "kmonitor report QPS => " << metric_str << " value => "
                       << value;
  return (PyObject *)Py_BuildValue("s", "qps");
}

static PyMethodDef PyStreamingKmonitor_methods[] = {
    {"start", (PyCFunction)PyStreamingKmonitor_start, METH_VARARGS, "start"},
    {"stop", (PyCFunction)PyStreamingKmonitor_stop, METH_VARARGS, "stop"},
    {"update_gauge", (PyCFunction)PyStreamingKmonitor_update_gauge, METH_VARARGS,
     "update_gauge"},
    {"update_counter", (PyCFunction)PyStreamingKmonitor_update_counter, METH_VARARGS,
     "update_counter"},
    {"update_qps", (PyCFunction)PyStreamingKmonitor_update_qps, METH_VARARGS,
     "update_qps"},
    {NULL} /* Sentinel */
};

static int PyStreamingKmonitor_init(PyStreamingKmonitor *self, PyObject *args,
                                    PyObject *kwds) {
  return 0;
}

static void PyStreamingKmonitor_dealloc(PyStreamingKmonitor *self) {
  Py_TYPE(self)->tp_free((PyObject *)self);
}

PyTypeObject PyStreamingKmonitorType = {
    PyVarObject_HEAD_INIT(NULL, 0)           /* ob_size */
    "streaming.StreamingKmonitorClient",     /* tp_name */
    sizeof(PyStreamingKmonitor),             /* tp_basicsize */
    0,                                       /* tp_itemsize */
    (destructor)PyStreamingKmonitor_dealloc, /* tp_dealloc */
    0,                                       /* tp_print */
    0,                                       /* tp_getattr */
    0,                                       /* tp_setattr */
    0,                                       /* tp_compare */
    0,                                       /* tp_repr */
    0,                                       /* tp_as_number */
    0,                                       /* tp_as_sequence */
    0,                                       /* tp_as_mapping */
    0,                                       /* tp_hash */
    0,                                       /* tp_call */
    0,                                       /* tp_str */
    0,                                       /* tp_getattro */
    0,                                       /* tp_setattro */
    0,                                       /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,                      /* tp_flags */
    "Streaming Kmonitor Client object",      /* tp_doc */
    0,                                       /* tp_traverse */
    0,                                       /* tp_clear */
    0,                                       /* tp_richcompare */
    0,                                       /* tp_weaklistoffset */
    0,                                       /* tp_iter */
    0,                                       /* tp_iternext */
    PyStreamingKmonitor_methods,             /* tp_methods */
    0,                                       /* tp_members */
    0,                                       /* tp_getset */
    0,                                       /* tp_base */
    0,                                       /* tp_dict */
    0,                                       /* tp_descr_get */
    0,                                       /* tp_descr_set */
    0,                                       /* tp_dictoffset */
    (initproc)PyStreamingKmonitor_init,      /* tp_init */
    0,                                       /* tp_alloc */
    PyType_GenericNew,                       /* tp_new */
};

#if PY_MAJOR_VERSION >= 3
static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    "libstreaming_kmonitor",                /* m_name */
    "A module for the streaming kmonitor.", /* m_doc */
    0,                                      /* m_size */
    streaming_kmonitor_methods,             /* m_methods */
    NULL,                                   /* m_reload */
    NULL,                                   /* m_traverse */
    NULL,                                   /* m_clear */
    NULL,                                   /* m_free */
};
#endif

#if PY_MAJOR_VERSION >= 3
#define INITERROR return NULL
#else
#define INITERROR return
#endif

#ifndef PyMODINIT_FUNC /* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif
#if PY_MAJOR_VERSION >= 3
#define MOD_INIT(name) PyMODINIT_FUNC PyInit_##name(void)
#else
#define MOD_INIT(name) PyMODINIT_FUNC init##name(void)
#endif

MOD_INIT(libstreaming_kmonitor) {
  if (PyType_Ready(&PyStreamingKmonitorType) < 0) {
    INITERROR;
  }
#if PY_MAJOR_VERSION >= 3
  PyObject *m = PyModule_Create(&moduledef);
#else
  PyObject *m = Py_InitModule3("libstreaming_kmonitor", streaming_kmonitor_methods,
                               "A module for the streaming kmonitor.");
#endif
  Py_INCREF(&PyStreamingKmonitorType);
  PyModule_AddObject(m, "StreamingKmonitor", (PyObject *)&PyStreamingKmonitorType);
#if PY_MAJOR_VERSION >= 3
  return m;
#endif
}
