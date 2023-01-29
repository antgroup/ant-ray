#pragma once
#include "api/function.h"

ray::streaming::Function *CreateLambdaExampleSourceFunction();
ray::streaming::Function *CreateExampleSourceFunction();
ray::streaming::Function *CreateLambdaExampleMapFunction();
ray::streaming::Function *CreateLambdaExampleSinkFunction();

STREAMING_FUNC_ALIAS(::CreateExampleSourceFunction, CreateExampleSourceFunction);
STREAMING_FUNC_ALIAS(::CreateLambdaExampleSourceFunction,
                     CreateLambdaExampleSourceFunction);
STREAMING_FUNC_ALIAS(::CreateLambdaExampleMapFunction, CreateLambdaExampleMapFunction);
STREAMING_FUNC_ALIAS(::CreateLambdaExampleSinkFunction, CreateLambdaExampleSinkFunction);