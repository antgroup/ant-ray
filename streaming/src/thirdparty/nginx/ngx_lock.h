#ifndef _NGX_LOCK_H_
#define _NGX_LOCK_H_

#define ngx_shmtx_lock(x)   { /*void*/ }
#define ngx_shmtx_unlock(x) { /*void*/ }

typedef struct {
  ngx_uint_t spin;
} ngx_shmtx_t;

typedef struct {
  ngx_uint_t spin;
} ngx_shmtx_sh_t;

#endif