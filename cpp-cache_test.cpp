//
// Created by lrleon on 11/8/24.
//

# include <iostream>
#include <utility>
# include <gtest/gtest.h>
# include "cpp-cache.H"

using namespace std;
using namespace testing;

size_t hash_fct(const int &key)
{
  return key;
}

struct SimpleFixture : public Test
{
  static bool miss_handler(const int &key, int *data, void *user_data)
  {
    return true;
  }

  Cache<int, int> cache;

  SimpleFixture()
    : cache(10, miss_handler, hash_fct)
  {
    // empty
  }
};

TEST_F(SimpleFixture, test_hash_fct)
{
  ASSERT_EQ(hash_fct(10), 10);
  Cache<int, int>::CacheEntry cache_entry;
    cache_entry.set_key(10);
}

TEST(cache_entry, basic)
{
  using Cache = Cache<int, int>;
  Cache::CacheEntry cache_entry;

  ASSERT_EQ(cache_entry.status(),
            Cache::CacheEntry::Status::AVAILABLE);
  ASSERT_TRUE(cache_entry.link_lru()->is_empty());
  ASSERT_EQ(cache_entry.get_ad_hoc_code(), 0);

  cache_entry.set_data(10);

  ASSERT_EQ(cache_entry.get_data(), 10);

  cache_entry.set_status(Cache::CacheEntry::Status::READY);

  ASSERT_EQ(cache_entry.status(),
            Cache::CacheEntry::Status::READY);

  cache_entry.set_ad_hoc_code(1);

  ASSERT_EQ(cache_entry.get_ad_hoc_code(), 1);

  cache_entry.set_positive_ttl_exp_time(high_resolution_clock::now() + 1s);
  cache_entry.set_negative_ttl_exp_time(high_resolution_clock::now() + 2s);

  ASSERT_FALSE(cache_entry.positive_ttl_expired(high_resolution_clock::now()));
  ASSERT_FALSE(cache_entry.negative_ttl_expired(high_resolution_clock::now()));

  // sleep for 1 second
  sleep(1);

  ASSERT_TRUE(cache_entry.positive_ttl_expired(high_resolution_clock::now()));
  ASSERT_FALSE(cache_entry.negative_ttl_expired(high_resolution_clock::now()));

  sleep(1);

  ASSERT_TRUE(cache_entry.negative_ttl_expired(high_resolution_clock::now()));

  cout << "CacheEntry: " << cache_entry.get_data() << endl;
}

TEST(cache_entry, key_copy_works)
{
  Cache<vector<int>, int>::CacheEntry cache_entry;

  vector<int> key = {1, 2, 3};
  cache_entry.set_key(key);

  ASSERT_EQ(cache_entry.key(), key);
}

TEST(cache_entry, key_move_works)
{
  Cache<vector<int>, int>::CacheEntry cache_entry;

  vector<int> key = {1, 2, 3};
  cache_entry.set_key(move(key));

  ASSERT_EQ(cache_entry.key().size(), 3);
  ASSERT_EQ(cache_entry.key(), vector<int>({1, 2, 3}));
  ASSERT_EQ(key.size(), 0);
  ASSERT_TRUE(key.empty());
}

TEST(cache_entry, data_copy_works)
{
  Cache<int, vector<int>>::CacheEntry cache_entry;

  vector<int> data = {1, 2, 3};
  cache_entry.set_data(data);

  ASSERT_EQ(cache_entry.data(), data);
}

TEST(cache_entry, data_move_works)
{
  Cache<int, vector<int>>::CacheEntry cache_entry;

  vector<int> data = {1, 2, 3};
  cache_entry.set_data(move(data));

  ASSERT_EQ(cache_entry.data().size(), 3);
  ASSERT_EQ(cache_entry.data(), vector<int>({1, 2, 3}));
  ASSERT_EQ(data.size(), 0);
  ASSERT_TRUE(data.empty());

  OLhashTable<int, vector<int>> hash_table(10);
}


