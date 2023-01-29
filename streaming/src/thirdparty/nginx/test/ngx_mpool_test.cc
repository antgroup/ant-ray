#include "ngx_slab.h"
#include "gtest/gtest.h"

TEST(ngx_memory_pool, test1)
{
  size_t 	pool_size = 40960000;  //4M
  u_char 	*space;
  space = (u_char *)malloc(pool_size);

  ngx_slab_pool_t *sp;
  sp = (ngx_slab_pool_t*) space;

  sp->addr = space;
  sp->min_shift = 3;
  sp->end = space + pool_size;

  ngx_slab_sizes_init();
  ngx_slab_init(sp);

  for (int i = 0; i < 1000000; i++)
  {
    size_t  size = 128 + i % 4096;
    char *p = (char *)ngx_slab_alloc(sp, size);
    if (p == NULL)
    {
      EXPECT_TRUE(false);
    }
    memset(p, 0, size);
    ngx_slab_free(sp, p);
  }
  free(space);
}

TEST(ngx_memory_pool, test2)
{
  char *p;
  for (int i = 0; i < 1000000; i++)
  {
    size_t size = 128 + (i % 4096);
    p = (char *)malloc(size);
    memset(p, 0, size);
    free(p);
  }
}

TEST(ngx_memory_pool, test3)
{
  for (int i = 0; i < 1000000; i++)
  {
    size_t size = 128 + (i + 4096);
    char *p = new char[size];
    memset(p, 0, size);
    delete[] p;
  }
}
int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
