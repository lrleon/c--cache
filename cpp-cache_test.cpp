//
// Created by lrleon on 11/8/24.
//

# include <iostream>
# include <utility>
# include <gtest/gtest.h>
# include <future>
# include "cpp-cache.H"

# include <tpl_dynMapTree.H>

using namespace std;
using namespace testing;

TEST(cache_entry, basic)
{
  using Cache = Cache<int, int>;
  Cache::CacheEntry cache_entry;

  ASSERT_EQ(cache_entry.status(),
            Cache::CacheEntry::Status::AVAILABLE);
  ASSERT_TRUE(cache_entry.link_lru()->is_empty());
  ASSERT_EQ(cache_entry.ad_hoc_code(), 0);

  cache_entry.set_data(10);

  ASSERT_EQ(cache_entry.get_data(), 10);

  cache_entry.set_status(Cache::CacheEntry::Status::READY);

  ASSERT_EQ(cache_entry.status(),
            Cache::CacheEntry::Status::READY);

  cache_entry.ad_hoc_code() = 1;

  ASSERT_EQ(cache_entry.ad_hoc_code(), 1);

  cache_entry.set_ttl_exp_time(high_resolution_clock::now() + 1s);


  ASSERT_FALSE(cache_entry.has_ttl_expired(high_resolution_clock::now()));

  // sleep for 1 second
  std::this_thread::sleep_for(std::chrono::seconds(1));

  ASSERT_TRUE(cache_entry.has_ttl_expired(high_resolution_clock::now()));

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

///template <typename ... Args>
struct SimpleFixture : public Test
{
  //template <typename ... Args>
  static bool miss_handler(const int &key, int *data,
                           int8_t &ad_hoc_code /*,
                           Args ... args */)
  {
    *data = key * 10;
    ++ad_hoc_code; // never must be greater than 1
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

  // check expired key was removed from the cache
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
  ASSERT_NE(cache.insert(1, 10), nullptr)
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

TEST_F(SimpleFixture, remove)
{
  cache.insert(1, 10);
  cache.insert(2, 20);

  ASSERT_EQ(cache.size(), 2);

  ASSERT_TRUE(cache.has(1));

  cache.remove(1);

  ASSERT_EQ(cache.size(), 1);
  ASSERT_FALSE(cache.has(1));
}

TEST_F(SimpleFixture, retrieve_or_compute_basic)
{
  auto res = cache.retrieve_from_cache_or_compute(1);

  ASSERT_EQ(cache.size(), 1);
  ASSERT_TRUE(cache.has(1));
  ASSERT_EQ(*res.first, 10);
  ASSERT_EQ(res.second, 1);

  for (int i = 1; i < 10; ++i)
    {
      res = cache.retrieve_from_cache_or_compute(1);

      ASSERT_EQ(cache.size(), 1);
      ASSERT_TRUE(cache.has(1));
      (*res.first)++;
      ASSERT_EQ(*res.first, 10 + i);
      ASSERT_EQ(res.second, 1);
    }
}

TEST_F(SimpleFixture, get_cache_entry)
{
  int *data = cache.insert(1, 10);

  auto cache_entry = Cache<int, int>::CacheEntry::to_CacheEntry(*data);

  ASSERT_EQ(cache_entry->key(), 1);
  ASSERT_EQ(cache_entry->get_data(), 10);
  ASSERT_EQ(&cache_entry->get_data(), data);
}

TEST_F(SimpleFixture, iterator)
{
  DynMapTree<int, int> key_value_map = {{1, 10},
                                        {2, 20},
                                        {3, 30},
                                        {4, 40},
                                        {5, 50}};
  cache.insert(1, 10);
  cache.insert(2, 20);
  cache.insert(3, 30);
  cache.insert(4, 40);
  cache.insert(5, 50);

  auto it = cache.get_it();
  ASSERT_TRUE(it.has_curr());
  auto kv = key_value_map.search(it.get_curr().first);
  ASSERT_NE(kv, nullptr);
  ASSERT_EQ(*it.get_curr().second, kv->second);

  it.next();
  ASSERT_TRUE(it.has_curr());
  kv = key_value_map.search(it.get_curr().first);
  ASSERT_NE(kv, nullptr);
  ASSERT_EQ(*it.get_curr().second, kv->second);

  it.next();
  ASSERT_TRUE(it.has_curr());
  kv = key_value_map.search(it.get_curr().first);
  ASSERT_NE(kv, nullptr);
  ASSERT_EQ(*it.get_curr().second, kv->second);

  it.next();
  ASSERT_TRUE(it.has_curr());
  kv = key_value_map.search(it.get_curr().first);
  ASSERT_NE(kv, nullptr);
  ASSERT_EQ(*it.get_curr().second, kv->second);
}

struct TimeConsumingFixture : public Test
{
  static bool miss_handler(const int &key, int *data,
                           int8_t &ad_hoc_code)
  {
    *data = key * 10;
    ++ad_hoc_code; // never must be greater than 1
    sleep(2);
    return true;
  }

  Cache<int, int> cache;

  TimeConsumingFixture()
    : cache(5, 3s, 1s, miss_handler)
  {
    // empty
  }
};

TEST_F(TimeConsumingFixture, calculating_status_while_computing)
{
  using CacheEntry = Cache<int, int>::CacheEntry;
  CacheEntry entry(1);
  CacheEntry *cache_entry = &entry;
  auto future =
    std::async(std::launch::async, [this, &cache_entry]() mutable
    {
      return cache.resolve_cache_miss(cache_entry,
                                      high_resolution_clock::now());
    });

  // wait 1s
  sleep(1);
  cout << CacheEntry::status_to_string(cache_entry->status()) << endl;
  ASSERT_EQ(cache_entry->status(), CacheEntry::Status::CALCULATING);

  // wait miss handler to finish
  pair<int *, int8_t> res = future.get();

  cout << CacheEntry::status_to_string(cache_entry->status()) << endl;
  ASSERT_EQ(cache_entry->status(), CacheEntry::Status::READY);
  ASSERT_FALSE(cache_entry->has_ttl_expired(high_resolution_clock::now()));

  ASSERT_EQ(*res.first, 10);
  ASSERT_EQ(res.second, 1);
  ASSERT_EQ(cache_entry->data(), 10);
}

TEST_F(TimeConsumingFixture, two_threads)
{
  auto future1 = std::async(std::launch::async, [this]()
  {
    return cache.retrieve_from_cache_or_compute(1);
  });

  auto future2 = std::async(std::launch::async, [this]()
  {
    return cache.retrieve_from_cache_or_compute(1);
  });

  auto res1 = future1.get();
  auto res2 = future2.get();

  ASSERT_EQ(cache.size(), 1);
  ASSERT_TRUE(cache.has(1));

  ASSERT_EQ(*res1.first, 10);
  ASSERT_EQ(res1.second, 1);

  ASSERT_EQ(res1.first, res2.first); // same address
  ASSERT_EQ(res1.second, res2.second);

}

TEST_F(TimeConsumingFixture, multithread_cache_full)
{
  constexpr int N = 3;
  vector<future<pair<int *, int8_t>>> futures;

  for (int i = 1; i <= 5; ++i)
    {
      for (int j = 0; j < N; ++j)
        {
          futures.push_back(std::async(std::launch::async, [this, i]()
          {
            return cache.retrieve_from_cache_or_compute(i);
          }));
        }
    }

  vector<pair<int *, int8_t>> results;
  for (int i = 0; i < N * 5; ++i)
    results.push_back(futures[i].get());

  ASSERT_EQ(cache.size(), 5);

  for (int i = 1; i <= 5; ++i)
    ASSERT_TRUE(cache.has(i));

  for (int i = 0; i < N * 5; i += N)
    {
      auto res_i = results[i];
      for (int j = 1; j < N; ++j)
        {
          auto res_j = results[i + j];
          ASSERT_EQ(res_i.first, res_j.first); // same address
          ASSERT_EQ(res_i.second, res_j.second);
        }
    }

}

TEST_F(TimeConsumingFixture, multithread_heavy_futures)
{
  constexpr int N = 100;
  vector<future<pair<int *, int8_t>>> futures;

  for (int i = 1; i <= 5; ++i)
    {
      for (int j = 0; j < N; ++j)
        {
          futures.push_back(std::async(std::launch::async, [this, i]()
          {
            return cache.retrieve_from_cache_or_compute(i);
          }));
        }
    }

  vector<pair<int *, int8_t>> results;
  for (int i = 0; i < N * 5; ++i)
    results.push_back(futures[i].get());

  ASSERT_EQ(cache.size(), 5);

  for (int i = 1; i <= 5; ++i)
    ASSERT_TRUE(cache.has(i));

  for (int i = 0; i < N * 5; i += N)
    {
      auto res_i = results[i];
      for (int j = 1; j < N; ++j)
        {
          auto res_j = results[i + j];
          ASSERT_EQ(res_i.first, res_j.first); // same address
          ASSERT_EQ(res_i.second, res_j.second);
        }
    }
}

TEST_F(TimeConsumingFixture, multithread_heavy_threads)
{
  constexpr int N = 100;
  vector<thread> threads;
  vector<pair<int *, int8_t>> results(N * 5);
  mutex results_mutex;
  int result_index = 0;

  for (int i = 1; i <= 5; ++i)
    {
      for (int j = 0; j < N; ++j)
        {
          threads.emplace_back([this, i, &results, &results_mutex, &result_index]()
                               {
                                 auto result =
                                   cache.retrieve_from_cache_or_compute(i);
                                 {
                                   lock_guard<mutex> lock(results_mutex);
                                   results[result_index++] = result;
                                 }
                               });
        }
    }

  for (auto &t: threads)
    t.join();

  ASSERT_EQ(cache.size(), 5);

  for (int i = 1; i <= 5; ++i)
    ASSERT_TRUE(cache.has(i));

  for (int i = 0; i < N * 5; i += N)
    {
      auto res_i = results[i];
      for (int j = 1; j < N; ++j)
        {
          auto res_j = results[i + j];
          ASSERT_EQ(res_i.first, res_j.first); // same address
          ASSERT_EQ(res_i.second, res_j.second);
        }
    }
}




