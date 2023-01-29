#ifndef _H_PANGU1_API_
#define _H_PANGU1_API_
/* Explicitly enable the UINT64_MAX macros */
#ifndef __STDC_LIMIT_MACROS
#define __STDC_LIMIT_MACROS  1
#endif
#include <stdint.h>
#include <stdio.h>  // for SEEK_SET, SEEK_CUR, SEEK_END

#ifdef __cplusplus
extern "C" {
#endif
typedef struct _file_status {
    uint64_t file_length;		// file length
    int is_dir;				// 1 indicate it is a dir, 0 is file
    int copys;				// number of min replica for the file
    uint64_t create_time;		// create time of the file. From 1970.1.1
    uint64_t modified_time;		// modify time for the file
    // newly added to support NFS
    uint64_t file_id;			// file id, could be deemed as inode id
    uint32_t hardlinks;			// number of hard links of the file
    int file_flag;			// file flag, internal usage only now
    uint8_t file_attr;			// file attr, internal usage only
    uint16_t access;                    // access permission of the file
    uint32_t owner;                     // owner id of the file
    uint32_t group;                     // group id of the file
} file_status_t;

typedef struct _dir_status {
    uint64_t dir_count;
    uint64_t file_count;
    uint64_t space_size;
    int64_t space_quota;
    int64_t files_quota;
} dir_status_t;

typedef void* pangu_dir_t;
typedef struct _file_handle_obj {
    void* stream_obj;			// stream object for the file
    int file_type;			// file type defined below
    int rw_flags;			// read or write flag defined below
    int need_seek;			// whether need to do seek before read/write
    uint64_t offset;			// offset for the next read or write operation
} file_handle_obj, *file_handle_t;

typedef void* pangu_chunk_handle_t;
typedef struct _chunk_location {
#define MAX_CHUNK_LOC_SERVERS	64
    uint64_t chunk_attr;      		// chunk attribute such as EC, etc
    char* chunk_server[MAX_CHUNK_LOC_SERVERS];   // list of hostname for chunk server
    uint64_t block_offset;    		// offset for the chunk start in the file
    uint64_t block_length;    		// in pangu, it is chunk size
} chunk_location_t;

// open flag
enum {
    FLAG_GENERIC_READ           = 0x1,
    FLAG_GENERIC_WRITE          = 0x2,  // not implemented
    FLAG_SEQUENTIAL_READ        = 0x4,
    FLAG_SEQUENTIAL_WRITE       = 0x8,
    FLAG_READ_ONE_LOG           = 0x10,
    FLAG_READ_BATCH_LOG         = 0x20,
    FLAG_READ_LOG_WITH_CHECKSUM = 0x40,
    FLAG_READ_WITH_BACKUP1      = 0x80,
    FLAG_WRITE_USE_CACHE        = 0x100,
    FLAG_IO_PRIORITY_P1         = 0x1000, // Latency-Sensitive
    FLAG_IO_PRIORITY_P2         = 0x2000, // Throughput-Sensitive
    FLAG_IO_PRIORITY_P3         = 0x4000  // Best-Effort
};

enum {
    FILE_TYPE_NORMAL = 0,
    FILE_TYPE_RECORD = 1,	// not implemented
    FILE_TYPE_LOGFILE = 2,
    FILE_TYPE_RAIDFILE = 3
};

#ifndef PANGU_SDK_DYNAMIC_LOAD

int pangu_init(const char* uri, int perm);

int pangu_uninit();

int pangu_set_user_group(uint32_t owner, uint32_t group);

int pangu_create(const char* path, int min_copys, int max_copys, const char* app_name,
                 const char* part_name, int overwrite, int mode);

int pangu_create1(const char* path, int min_copys, int max_copys, const char* app_name,
                  const char* part_name, int overwrite, int mode, int file_type);

int pangu_open(const char* path, int flag, int mode, int file_type, file_handle_t* fhandle);

int pangu_close(file_handle_t fhandle);

int pangu_read(file_handle_t fhandle, char* buf, int size);

int pangu_read1(file_handle_t fhandle, char* buf, int size, int opt);

int pangu_write(file_handle_t fhandle, const char* buf, int size);

int pangu_write1(file_handle_t fhandle, const char* buf, int size, int opt);

int pangu_fsync(file_handle_t fhandle);

int64_t pangu_lseek(file_handle_t fhandle, int64_t offset, int whence);

int pangu_remove(const char* path, int permanent);

int pangu_mkdir(const char* path, int mode);

int pangu_rmdir(const char* path, int permanent);

int pangu_dir_exist(const char* dir_path);

int pangu_file_exist(const char* file_path);

int pangu_get_status(const char* path, file_status_t* status);

int pangu_dir_status(const char* path, dir_status_t* status);

int pangu_open_dir(const char* dir_path, pangu_dir_t* dir_handle, int list_batch);

int pangu_read_dir(pangu_dir_t dir_handle, char* name, int* name_len, file_status_t* status);

int pangu_close_dir(pangu_dir_t dir_handle);

int pangu_seek_dir(pangu_dir_t dir_handle, const char* name);

int pangu_file_time(const char* file_path, uint64_t* create_time, uint64_t* mtime);

int pangu_file_length(const char* file_path, uint64_t* len);

int pangu_dir_length(const char* dir_path, uint64_t* len);

int pangu_append(const char* path, const char* buf, int size, int sync);

int pangu_pread(const char* path, char* buf, int size, uint64_t offset);

int pangu_truncate(const char* path, uint64_t new_size);

int pangu_rename_file(const char* src_name, const char* dst_name);

int pangu_rename_dir(const char* src_name, const char* dst_name);

int pangu_link(const char* src_name, const char* dst_name);

int pangu_chmod(const char* path, int mode);

int pangu_chown(const char* path, uint32_t owner, uint32_t group);

int pangu_utime(const char* path, uint64_t mtime);

int pangu_setxattr(const char* path, const char* name, const void* value,
                   int size, int flags);

int pangu_getxattr(const char* path, const char* name, void* value, int size);

int pangu_listxattr(const char* path, char* list, int size);

int pangu_open_block_location(const char* path, uint64_t offset, uint64_t length, pangu_chunk_handle_t* handle);

int pangu_next_block_location(pangu_chunk_handle_t handle, chunk_location_t* chunk_loc);

int pangu_close_block_location(pangu_chunk_handle_t handle);

int pangu_set_flag(const char* flag_name, const void* value, int size);

int pangu_get_flag(const char* flag_name, void* value, int size);

#else

namespace zdfs {

int pangu_init_lib();

int pangu_free_lib();

typedef int (*pangu_init_func)(const char*, int);
typedef int (*pangu_uninit_func)();
typedef int (*pangu_set_user_group_func)(uint32_t, uint32_t);
typedef int (*pangu_create1_func)(const char*, int, int, const char*, const char*, int, int, int);
typedef int (*pangu_open_func)(const char*, int, int, int, file_handle_t*);
typedef int (*pangu_close_func)(file_handle_t);
typedef int (*pangu_read1_func)(file_handle_t, char*, int, int);
typedef int (*pangu_write1_func)(file_handle_t, const char*, int, int);
typedef int (*pangu_fsync_func)(file_handle_t);
typedef int64_t (*pangu_lseek_func)(file_handle_t, int64_t, int);
typedef int (*pangu_remove_func)(const char*, int);
typedef int (*pangu_mkdir_func)(const char*, int);
typedef int (*pangu_rmdir_func)(const char*, int);
typedef int (*pangu_dir_exist_func)(const char*);
typedef int (*pangu_file_exist_func)(const char*);
typedef int (*pangu_get_status_func)(const char*, file_status_t*);
typedef int (*pangu_dir_status_func)(const char*, dir_status_t*);
typedef int (*pangu_open_dir_func)(const char*, pangu_dir_t*, int);
typedef int (*pangu_read_dir_func)(pangu_dir_t, char*, int*, file_status_t*);
typedef int (*pangu_close_dir_func)(pangu_dir_t);
typedef int (*pangu_seek_dir_func)(pangu_dir_t, const char*);
typedef int (*pangu_rename_file_func)(const char*, const char*);
typedef int (*pangu_rename_dir_func)(const char*, const char*);
typedef int (*pangu_link_func)(const char*, const char*);
typedef int (*pangu_chmod_func)(const char*, int);
typedef int (*pangu_chown_func)(const char*, uint32_t, uint32_t);
typedef int (*pangu_set_flag_func)(const char*, const void*, int);
typedef int (*pangu_get_flag_func)(const char*, void*, int);

__attribute__ ((visibility("hidden"))) extern pangu_init_func pangu_init;
__attribute__ ((visibility("hidden"))) extern pangu_uninit_func pangu_uninit;
__attribute__ ((visibility("hidden"))) extern pangu_set_user_group_func pangu_set_user_group;
__attribute__ ((visibility("hidden"))) extern pangu_create1_func pangu_create1;
__attribute__ ((visibility("hidden"))) extern pangu_open_func pangu_open;
__attribute__ ((visibility("hidden"))) extern pangu_close_func pangu_close;
__attribute__ ((visibility("hidden"))) extern pangu_read1_func pangu_read1;
__attribute__ ((visibility("hidden"))) extern pangu_write1_func pangu_write1;
__attribute__ ((visibility("hidden"))) extern pangu_fsync_func pangu_fsync;
__attribute__ ((visibility("hidden"))) extern pangu_lseek_func pangu_lseek;
__attribute__ ((visibility("hidden"))) extern pangu_remove_func pangu_remove;
__attribute__ ((visibility("hidden"))) extern pangu_mkdir_func pangu_mkdir;
__attribute__ ((visibility("hidden"))) extern pangu_rmdir_func pangu_rmdir;
__attribute__ ((visibility("hidden"))) extern pangu_dir_exist_func pangu_dir_exist;
__attribute__ ((visibility("hidden"))) extern pangu_file_exist_func pangu_file_exist;
__attribute__ ((visibility("hidden"))) extern pangu_get_status_func pangu_get_status;
__attribute__ ((visibility("hidden"))) extern pangu_dir_status_func pangu_dir_status;
__attribute__ ((visibility("hidden"))) extern pangu_open_dir_func pangu_open_dir;
__attribute__ ((visibility("hidden"))) extern pangu_read_dir_func pangu_read_dir;
__attribute__ ((visibility("hidden"))) extern pangu_close_dir_func pangu_close_dir;
__attribute__ ((visibility("hidden"))) extern pangu_seek_dir_func pangu_seek_dir;
__attribute__ ((visibility("hidden"))) extern pangu_rename_file_func pangu_rename_file;
__attribute__ ((visibility("hidden"))) extern pangu_rename_dir_func pangu_rename_dir;
__attribute__ ((visibility("hidden"))) extern pangu_link_func pangu_link;
__attribute__ ((visibility("hidden"))) extern pangu_chmod_func pangu_chmod;
__attribute__ ((visibility("hidden"))) extern pangu_chown_func pangu_chown;
__attribute__ ((visibility("hidden"))) extern pangu_set_flag_func pangu_set_flag;
__attribute__ ((visibility("hidden"))) extern pangu_get_flag_func pangu_get_flag;

} // namespace zdfs
#endif // PANGU_SDK_DYNAMIC_LOAD

#ifdef __cplusplus
}
#endif

#endif // _H_PANGU1_API_
