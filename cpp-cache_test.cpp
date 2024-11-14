//
// Created by lrleon on 11/8/24.
//

# include <iostream>
#include <utility>
# include <gtest/gtest.h>
# include "cpp-cache.H"

using namespace std;
using namespace testing;

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
  cache_entry.set_data(std::move(data));

  ASSERT_EQ(cache_entry.data().size(), 3);
  ASSERT_EQ(cache_entry.data(), vector<int>({1, 2, 3}));
  ASSERT_EQ(data.size(), 0);
  ASSERT_TRUE(data.empty());
}

struct SimpleFixture : public Test
{
  static bool miss_handler(const int &key, int *data, void *user_data)
  {
    return true;
  }

  Cache<int, int> cache;

  SimpleFixture()
    : cache(5, 1s, 1s, miss_handler)
  {
    // empty
  }
};

TEST_F(SimpleFixture, basic)
{
  ASSERT_EQ(cache.capacity(), 5);
  ASSERT_EQ(cache.size(), 0);
  //cache is empty
  ASSERT_FALSE(cache.has(1));
  //insert a key-value pair
  cache.insert(1, 10);
  //cache is not empty
  ASSERT_EQ(cache.size(), 1);
  // check lru entry
  ASSERT_EQ(cache.get_lru_entry()->key(), 1);
  //key is in cache
  ASSERT_TRUE(cache.has(1));
  // wait ttl to expire
  sleep(1);
  // key is not in cache
  ASSERT_FALSE(cache.has(1));
  // check expired key was removed from cache
  ASSERT_EQ(cache.size(), 0);
}

TEST_F(SimpleFixture, lru)
{
  cache.insert(1, 10);
  cache.insert(2, 20);

  ASSERT_EQ(cache.size(), 2);

  // check lru entry
  ASSERT_EQ(cache.get_lru_entry()->key(), 1);
  ASSERT_NE(cache.get_lru_entry()->key(), 90);

  // remove lru entry
  cache.remove_entry_from_hash_table(cache.get_lru_entry());

  ASSERT_EQ(cache.size(), 1);

  // check lru entry
  ASSERT_EQ(cache.get_lru_entry()->key(), 2);
}

TEST_F(SimpleFixture, insert_with_has_and_touch)
{
  ASSERT_TRUE(cache.insert(1, 10))
            << "It should be able to insert a key-value pair";

  ASSERT_TRUE(cache.has(1))
            << "It should be able to find the key";

  {
    auto p_lru = cache.get_lru();
    ASSERT_EQ(p_lru.first, 1);
    ASSERT_EQ(p_lru.second, 10);

    auto p_mru = cache.get_mru();
    ASSERT_EQ(p_mru.first, 1);
    ASSERT_EQ(p_mru.second, 10);

    // test positive key expiration
    sleep(1);
    ASSERT_FALSE(cache.has(1))
              << "It should not be able to find the key, since it has expired";
  }

  ASSERT_TRUE(cache.insert(1, 10))
            << "It should be able to insert a key-value pair";
  ASSERT_TRUE(cache.insert(2, 20))
            << "It should be able to insert a key-value pair";
  ASSERT_TRUE(cache.insert(3, 30))
            << "It should be able to insert a key-value pair";

  {
    auto p_lru = cache.get_lru();
    ASSERT_EQ(p_lru.first, 1);
    ASSERT_EQ(p_lru.second, 10);

    auto p_mru = cache.get_mru();
    ASSERT_EQ(p_mru.first, 3);
    ASSERT_EQ(p_mru.second, 30);
  }

  ASSERT_TRUE(cache.touch(2))
            << "It should be able to touch the key";

  auto p_mru = cache.get_mru();
  ASSERT_EQ(p_mru.first, 2);
  ASSERT_EQ(p_mru.second, 20);

  auto p_lru = cache.get_lru();
  ASSERT_EQ(p_lru.first, 1);
  ASSERT_EQ(p_lru.second, 10);
}

TEST_F(SimpleFixture, touch)
{
  cache.insert(1, 10);
  cache.insert(2, 20);

  ASSERT_EQ(cache.size(), 2);

  // touch key 1
  ASSERT_TRUE(cache.touch(1));
  // check lru entry
  ASSERT_EQ(cache.get_lru_entry()->key(), 2);
  ASSERT_NE(cache.get_lru_entry()->key(), 1);
  // touch unknown key
  ASSERT_FALSE(cache.touch(90));

}

TEST_F(SimpleFixture, cache_is_full)
{
  cache.insert(1, 10);
  cache.insert(2, 20);
  cache.insert(3, 30);
  cache.insert(4, 40);
  cache.insert(5, 50);

  ASSERT_EQ(cache.size(), 5);

  {
    auto p_lru = cache.get_lru();
    ASSERT_EQ(p_lru.first, 1);
    ASSERT_EQ(p_lru.second, 10);

    auto p_mru = cache.get_mru();
    ASSERT_EQ(p_mru.first, 5);
    ASSERT_EQ(p_mru.second, 50);
  }

  // insert a new key
  cache.insert(6, 60);

  ASSERT_EQ(cache.size(), 5);

  {
    auto p_lru = cache.get_lru();
    ASSERT_EQ(p_lru.first, 2);
    ASSERT_EQ(p_lru.second, 20);

    auto p_mru = cache.get_mru();
    ASSERT_EQ(p_mru.first, 6);
    ASSERT_EQ(p_mru.second, 60);
  }

  ASSERT_FALSE(cache.has(1));

}

