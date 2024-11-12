//
// Created by lrleon on 11/4/24.
//

#ifndef CPP_CACHE_CACHE_H
#define CPP_CACHE_CACHE_H

# include <chrono>
# include <memory>
# include <mutex>
# include <condition_variable>
# include <aleph.H>
# include <tpl_dnode.H>

# include <tpl_olhash.H>

#include <utility>

# include <gtest/gtest.h>

using namespace std;
using namespace Aleph;
using namespace std::chrono;


/* This is an implementation of a table-based associative cache.
   hash. The cache handles <Key, Data> pairs where Key is the key
   associative and Data is the data related to Key. are not allowed
   duplicate <Key, Data> pairs, but it is possible to have key
   duplicates.

   The cache has a size defined by cache_size specified in the
   builder. When the number of pairs inserted into the cache
   reaches cache_size, then the cache is said to be full. If
   tries to insert a new pair into a full cache, then it must
   delete a pair. In this implementation, the minus torque is eliminated
   recently used (lru).

   The implementation is based on a hash table with resolution of
   collisions by separate chaining. Each bucket in the table
   stores the pair. Additionally, the bucket contains a link that
   acts as a connector to a doubly linked list that simulates the
   lru order. Additionally, the bucket contains a direct link to the cache
   used to update your statistics.

   Pairs can be "locked"; that is: when a pair is
   blocked, it cannot be removed from the cache until it is
   released. A locked bucket will never be selected for
   replacement by lru policy.
*/

template <class Key, class Data, class Cmp = std::equal_to<Key>>
class Cache
{

public:
  
  class Entry
  {
    FRIEND_TEST(cache_entry, basic);
    
  private:
    
    Key _key;
    Data _data;
    
  public:
    
    Entry(const Key & k, const Data & d)
      : _key(k), _data(d)
    {
      // empty
    }
    
    Entry(Key && k, Data && d)
      : _key(std::move(k)), _data(std::move(d))
    {
      // empty
    }
    
    Entry() = default;
    
    const Key & key() const { return _key; }
    const Data & data() const { return _data; }
    
    void set_key(const Key & k) { _key = k; }
    void set_data(const Data & d) { _data = d; }
    
    void set_key(Key && k) { _key = std::move(k); }
    void set_data(Data && d) { _data = std::move(d); }
  }; // end class Entry
  
  class CacheEntry : public Entry
  {
    FRIEND_TEST(cache_entry, basic);

    enum class Status
    {
      AVAILABLE, CALCULATING, READY, FAILED
    };

    Dlink _dlink_lru; // dlink to lru queue

    mutex _mtx; // protects the CacheEntry while the calculation of the data is being done
    condition_variable _waiting_cv; // used for wake-up invoker waiting for the data is ready

    uint8_t _status = static_cast<uint8_t>(Status::AVAILABLE); // status of the cache entry (AVAILABLE, CALCULATING, READY, FAILED)
    int8_t _ad_hoc_code = 0; // ad hoc code to be used by the user for indicating their own codes

    time_point<high_resolution_clock> _positive_ttl_exp_time; // when positive ttl expires
    time_point<high_resolution_clock> _negative_ttl_exp_time; // when negative ttl expires

  public:

    CacheEntry() = default;

    Data get_data()
    { return this->data(); }

    Dlink *link_lru()
    { return &_dlink_lru; }

    mutex &mtx()
    { return _mtx; }

    condition_variable &waiting_cv()
    { return _waiting_cv; }

    Status status()
    { return static_cast<Status>(_status); }

    void set_status(const Status &status)
    {
      _status = to_underlying(status);
    }

    int8_t get_ad_hoc_code()
    { return _ad_hoc_code; }

    void set_ad_hoc_code(int8_t code)
    { _ad_hoc_code = code; }

    static size_t hash_fct(const Key &key)
    {
      return SuperFastHash(reinterpret_cast<void *>(&key), sizeof(key));
    }

    time_point<high_resolution_clock> positive_ttl_exp_time()
    { return _positive_ttl_exp_time; }
    time_point<high_resolution_clock> negative_ttl_exp_time()
    { return _negative_ttl_exp_time; }

    bool positive_ttl_expired(const high_resolution_clock::time_point &now) const
    {
      return now > _positive_ttl_exp_time;
    }

    bool negative_ttl_expired(const high_resolution_clock::time_point &now) const
    {
      return now > _negative_ttl_exp_time;
    }

    void set_positive_ttl_exp_time(const time_point<high_resolution_clock> & exp_time)
    {
      _positive_ttl_exp_time = exp_time;
    }

    void set_negative_ttl_exp_time(const time_point<high_resolution_clock> & exp_time)
    {
      _negative_ttl_exp_time = exp_time;
    }
  }; // end class CacheEntry

private:
  
  Dlink           lru_list; // lru list
  
  OLhashTable<CacheEntry, Cmp> hash_table;
  
  size_t          cache_size;  // cache length; MUST less than hash_table.capacity()
  
  mutex           mtx; // protects the cache

public:
  
  const size_t & get_num_entries() const
  {
    assert(hash_table.size() <= cache_size);
    
    return hash_table.size();
  }

protected:
  
  LINKNAME_TO_TYPE(CacheEntry, dlink_lru);
  
  void insert_entry_to_lru_list(CacheEntry * cache_entry)
  {
    lru_list.insert(cache_entry->link_lru());
  }
  
  void remove_entry_from_lru_list(CacheEntry * cache_entry)
  {
    cache_entry->link_lru()->del();
  }
  
  void move_to_inside_front(CacheEntry * cache_entry)
  {
    cache_entry->link_inside()->del();
  }
  
  void move_to_lru_front(CacheEntry * cache_entry)
  {
    cache_entry->link_lru()->del();
    lru_list.insert(cache_entry->link_lru());
  }
  
  void move_to_lru_rear(CacheEntry * cache_entry)
  {
    cache_entry->link_lru()->del();
    lru_list.append(cache_entry->link_lru());
  }
  
  void do_mru(CacheEntry * cache_entry)
  {
    move_to_lru_front(cache_entry);
  }
  
  void do_lru(CacheEntry * cache_entry)
  {
    move_to_lru_rear(cache_entry);
  }
  
  /*
    removes from hash table and makes the entry the least recently used
  */
  void remove_entry_from_hash_table(CacheEntry * cache_entry)
  {
    assert(not cache_entry->is_locked());
    
    cache_entry->link_inside()->del();
    
    hash_table.remove(cache_entry);
  }
  
  /* searches and returns the next entry according to lru priority that does not have
     a lock, removes from the hash table and makes the entry the most
     recently used.
  */
  CacheEntry * get_lru_entry()
  {
    assert(hash_table.size() <= cache_size);
    
    if (lru_list.is_empty())
      throw std::bad_alloc ();
    
    Dlink * lru_entry_link = lru_list.get_prev();
    CacheEntry * cache_entry = dlink_lru_to_CacheEntry(lru_entry_link);
    
    return cache_entry;
  }
  
public:

  using MissHandler = bool(const Key &, Data *, void *); // ad hoc pointer for receiving user data

  Cache(const size_t & size,
        std::function<bool(const Key &, Data *, void *)> miss_handler,
        size_t (*hash_fct)(const Key&) = CacheEntry::hash_fct)
    : hash_table(size, hash_fct),
      cache_size(hash_table.capacity())
  {
    assert(size > 1);
    assert(hash_fct != nullptr);
    
    auto * entries_array = new CacheEntry [cache_size];
  }

  bool has(const Key & key)
  {
    assert(hash_table.size() <= cache_size);
    
    auto * cache_entry =
      static_cast<CacheEntry*>(hash_table.search(key));
    
    if (cache_entry != nullptr)
    {
      do_mru(cache_entry);
      move_to_inside_front(cache_entry);
    }
    
    return cache_entry;
  }
  
  Data * retrieve_from_cache_or_compute(const Key& key)
  {
    assert(hash_table.size() <= cache_size);
    
    scoped_lock lock(mtx);
    
    // 1. Let entry be the result of a searching key in hash_table
    // 2. If found ==> return a pointer to the data
    // 3. If not found ==> compute the data and insert it into the cache
    
    // We need to block the invoking thread if the entry is found,
    // but its state is CALCULATING. Once the data is ready, we must
    // wake up the invoking thread.
  }
  
  void remove(const Key & key)
  {
  
  }
  
  const size_t & capacity() const { return cache_size; }
  
  const size_t & size() const { return hash_table.size(); }
  
  const size_t & get_num_busy_slots() const
  {
    return hash_table.get_num_busy_slots();
  }
  
  struct Iterator
  {
    Iterator(Cache& _cache)
    {
      // empty
    }
    
    CacheEntry * get_curr()
    {
      CacheEntry * ret_val = nullptr;
//        CacheEntry::dlink_inside_to_Cache_Entry(Dlink::Iterator::get_curr());
      
      assert(ret_val->is_in_table());
      
      return ret_val;
    }
  };
};



#endif // CPP_CACHE_CACHE_H
