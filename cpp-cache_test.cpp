//
// Created by lrleon on 11/8/24.
//

# include <iostream>
# include <gtest/gtest.h>
# include "cpp-cache.H"

using namespace std;
using namespace testing;

TEST(cache_entry, basic)
{
  using Cache = Cache<int, int>;
  Cache::CacheEntry cache_entry;
  
  ASSERT_EQ(cache_entry.status(),
            static_cast<uint8_t>(Cache::CacheEntry::Status::AVAILABLE));
  ASSERT_TRUE(cache_entry.link_lru()->is_empty());
  ASSERT_EQ(cache_entry.get_ad_hoc_code(), 0);
  
  cache_entry.set_data(10);
  
  ASSERT_EQ(cache_entry.get_data(), 10);
  
  
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