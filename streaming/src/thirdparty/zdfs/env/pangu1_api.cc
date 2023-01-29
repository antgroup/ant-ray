#include "pangu1_api.h"
#include <dlfcn.h>
#include <pthread.h>
#include <assert.h>

#ifdef PANGU_SDK_DYNAMIC_LOAD

namespace zdfs {
// define functions
pangu_init_func pangu_init = NULL;
pangu_uninit_func pangu_uninit = NULL;
pangu_set_user_group_func pangu_set_user_group = NULL;
pangu_create1_func pangu_create1 = NULL;
pangu_open_func pangu_open = NULL;
pangu_close_func pangu_close = NULL;
pangu_read1_func pangu_read1 = NULL;
pangu_write1_func pangu_write1 = NULL;
pangu_fsync_func pangu_fsync = NULL;
pangu_lseek_func pangu_lseek = NULL;
pangu_remove_func pangu_remove = NULL;
pangu_mkdir_func pangu_mkdir = NULL;
pangu_rmdir_func pangu_rmdir = NULL;
pangu_dir_exist_func pangu_dir_exist = NULL;
pangu_file_exist_func pangu_file_exist = NULL;
pangu_get_status_func pangu_get_status = NULL;
pangu_dir_status_func pangu_dir_status = NULL;
pangu_open_dir_func pangu_open_dir = NULL;
pangu_read_dir_func pangu_read_dir = NULL;
pangu_close_dir_func pangu_close_dir = NULL;
pangu_seek_dir_func pangu_seek_dir = NULL;
pangu_rename_file_func pangu_rename_file = NULL;
pangu_rename_dir_func pangu_rename_dir = NULL;
pangu_link_func pangu_link = NULL;
pangu_chmod_func pangu_chmod = NULL;
pangu_chown_func pangu_chown = NULL;
pangu_set_flag_func pangu_set_flag = NULL;
pangu_get_flag_func pangu_get_flag = NULL;


static void* lib_handle = NULL;

static pthread_mutex_t lib_ref_mutex = PTHREAD_MUTEX_INITIALIZER;

static long lib_ref_count = 0;

int pangu_init_lib()
{
    pthread_mutex_lock(&lib_ref_mutex);
    if (lib_ref_count > 0) {
        lib_ref_count++;
        pthread_mutex_unlock(&lib_ref_mutex);
        return 0;
    }

    lib_handle = dlopen("libpangu_api.so", RTLD_NOW);
    if (lib_handle == NULL) {
       printf("%s\n", dlerror());
       assert(lib_handle != NULL);
    }

#define DEFINE_SDK(handle, name) \
	name = (name##_func)dlsym(handle, #name); \
	assert(name != NULL);
    
    DEFINE_SDK(lib_handle, pangu_init)
    DEFINE_SDK(lib_handle, pangu_uninit)
    DEFINE_SDK(lib_handle, pangu_set_user_group)
    DEFINE_SDK(lib_handle, pangu_create1)
    DEFINE_SDK(lib_handle, pangu_open)
    DEFINE_SDK(lib_handle, pangu_close)
    DEFINE_SDK(lib_handle, pangu_read1)
    DEFINE_SDK(lib_handle, pangu_write1)
    DEFINE_SDK(lib_handle, pangu_fsync)
    DEFINE_SDK(lib_handle, pangu_lseek)
    DEFINE_SDK(lib_handle, pangu_remove)
    DEFINE_SDK(lib_handle, pangu_mkdir)
    DEFINE_SDK(lib_handle, pangu_rmdir)
    DEFINE_SDK(lib_handle, pangu_dir_exist)
    DEFINE_SDK(lib_handle, pangu_file_exist)
    DEFINE_SDK(lib_handle, pangu_get_status)
    DEFINE_SDK(lib_handle, pangu_dir_status)
    DEFINE_SDK(lib_handle, pangu_open_dir)
    DEFINE_SDK(lib_handle, pangu_read_dir)
    DEFINE_SDK(lib_handle, pangu_close_dir)

    /*
    DEFINE_SDK(lib_handle, pangu_seek_dir)
    */
    DEFINE_SDK(lib_handle, pangu_rename_file)
    DEFINE_SDK(lib_handle, pangu_rename_dir)
    DEFINE_SDK(lib_handle, pangu_link)
    DEFINE_SDK(lib_handle, pangu_chmod)
    DEFINE_SDK(lib_handle, pangu_chown)
    DEFINE_SDK(lib_handle, pangu_set_flag)
    DEFINE_SDK(lib_handle, pangu_get_flag)
#undef DEFINE_SDK

    lib_ref_count++;
    pthread_mutex_unlock(&lib_ref_mutex);
    return 0;
}

int pangu_free_lib()
{
    pthread_mutex_lock(&lib_ref_mutex);
    if (lib_ref_count == 1) {
        if (lib_handle) {
            dlclose(lib_handle);
            lib_handle = NULL;
        }
    }
    if (lib_ref_count > 0)
        lib_ref_count--;
    pthread_mutex_unlock(&lib_ref_mutex);
    return 0;
}
} // namespace zdfs
#endif // PANGU_SDK_DYNAMIC_LOAD
