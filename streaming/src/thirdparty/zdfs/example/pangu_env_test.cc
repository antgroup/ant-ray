#include "env/env.h"
#include <iostream>

using namespace zdfs;

int main(int argc, char* argv[])
{
    //
    // initialize pangu env
    //
    std::unique_ptr<Env> panguEnv;
    std::string clusterName = "pangu://localcluster";
    Status status = NewPanguEnv(&panguEnv, clusterName);
    if (!status.ok()) {
        std::cerr << status.ToString() << std::endl; 
        return 1;
    }

    //
    // Create a writable file
    //
    std::unique_ptr<WritableFile> writeFile;
    status = panguEnv->NewWritableFile("/zdfs_test/hello_world.txt",
                                       &writeFile,
                                       EnvOptions());

    if (!status.ok()) {
        std::cerr << status.ToString() << std::endl; 
        return 1;
    }

    //
    // Append some data and close it
    //
    writeFile->Append("Hello World\n");
    writeFile->Append("Hello Pangu\n");
    writeFile->Close();

    //
    // Read open this file
    //
    std::unique_ptr<RandomAccessFile> readFile;
    status = panguEnv->NewRandomAccessFile("/zdfs_test/hello_world.txt",
                                           &readFile,
                                           EnvOptions());
    if (!status.ok()) {
        std::cerr << status.ToString() << std::endl; 
        return 1;
    }

    uint64_t fileLength = 0;
    status = panguEnv->GetFileSize("/zdfs_test/hello_world.txt", &fileLength);
    std::cout << "file length is " << fileLength << std::endl;

    //
    // Read the data
    //
    int r_size = sizeof("Hello World\n") - 1;
    Slice out;
    char buffer[1024];
    readFile->Read(0, (r_size * 2), &out, buffer);
    if (status.ok()) {
        std::cout << out.ToString();
    }
    //
    // As we use unique_ptr, it will close the pangu env automatically
    return 0;
}
