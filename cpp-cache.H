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
# include "compression.H"

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

template<class Key, class Data, class Cmp = std::equal_to<Key>, typename ... Args>
class Cache
{
  FRIEND_TEST(SimpleFixture, basic);
  FRIEND_TEST(SimpleFixture, lru);
  FRIEND_TEST(SimpleFixture, touch);
  FRIEND_TEST(cache_entry, basic);
  FRIEND_TEST(cache_entry, key_copy_works);
  FRIEND_TEST(cache_entry, key_move_works);
  FRIEND_TEST(cache_entry, data_copy_works);
  FRIEND_TEST(cache_entry, data_move_works);
  FRIEND_TEST(SimpleFixture, get_cache_entry);
  FRIEND_TEST(TimeConsumingFixture, calculating_status_while_computing);
  FRIEND_TEST(CompressionFixture, basic_compression);
  FRIEND_TEST(CompressionFixture, retrieve_with_compression);

  class Entry
  {
   private:

    Key _key;
    Data * _data = nullptr;
    vector<char> _compressed_data = vector<char>();
    int _original_data_size = 0;

   public:

    Entry(const Key &k)
      : _key(k)
    {
      // empty
    }

    Entry(Key &&k)
      : _key(std::move(k))
    {
      // empty
    }

    Entry(const Entry &other)
      : _key(other._key), _data(other._data), 
        _compressed_data(other._compressed_data),
        _original_data_size(other._original_data_size)
    {
      // empty
    }

    Entry(Entry &&other) noexcept
      : _key(std::move(other._key)), _data(std::move(other._data)),
        _compressed_data(std::move(other._compressed_data)),
        _original_data_size(other._original_data_size)
    {
      // empty
    }

    Entry &operator=(const Entry &other)
    {
      if (this == &other)
        return *this;

      _key = other._key;
      _data = other._data;
      _compressed_data = other._compressed_data;
      _original_data_size = other._original_data_size;

      return *this;
    }

    Entry &operator=(Entry &&other) noexcept
    {
      if (this == &other)
        return *this;

      _key = std::move(other._key);
      _data = std::move(other._data);
      _compressed_data = std::move(other._compressed_data);
      _original_data_size =  std::move(other._original_data_size);

      return *this;
    }

    Entry() = default;

    const Key &key() const { return _key; }

    const Data &get_data() const { return *_data; }

    Data* data() { return _data; }

    vector<char>& compressed_data() { return _compressed_data; }

    void set_key(const Key &k) { _key = k; }

    void set_key(Key &&k) { _key = std::move(k); }

    void set_data(const Data & d) {
      _data = new Data(d);
    }    

    void set_data(Data && d) { 
      _data = new Data(std::move(d));
    }

    void set_data(Data * d) { _data = d; }

    void set_compressed_data(const vector<char> &compressed_data)
    {
      _compressed_data = compressed_data;
    }

    int& original_data_size() { return _original_data_size; }

    void swap(Entry &other) noexcept
    {
      using std::swap;
      swap(this->_key, other._key);
      swap(this->_data, other._data);
      swap(this->_compressed_data, other._compressed_data);
    }

    friend void swap(Entry &lhs, Entry &rhs) noexcept
    {
      using std::swap;
      swap(lhs._key, rhs._key);
      swap(lhs._data, rhs._data);
      swap(lhs._compressed_data, rhs._compressed_data);
    }

    // assuming data is a pointer to the field _data of Entry class,
    // it returns a pointer to Entry
    static Entry * to_Entry(Data & data)
    {
      return reinterpret_cast<Entry *>(reinterpret_cast<char *>(&data) - offsetof(Entry, _data));
    }
  }; // end class Entry

  class CacheEntry : public Entry
  {
    FRIEND_TEST(cache_entry, basic);

    friend struct SimpleFixture;

    friend class Cache<Key, Data, Cmp>;

    Dlink _dlink_lru; // dlink to lru queue

    mutex _mtx; // protects the CacheEntry while the calculation of the data is being done
    condition_variable _waiting_cv; // used for wake-up invoker waiting for the data is ready

    uint8_t _status = static_cast<uint8_t>(Status::AVAILABLE); // status of the cache entry (AVAILABLE, CALCULATING, READY, FAILED)
    int8_t _ad_hoc_code = 0; // ad hoc code to be used by the user for indicating their own codes

    time_point<high_resolution_clock> _positive_ttl_exp_time; // when positive ttl expires
    time_point<high_resolution_clock> _negative_ttl_exp_time; // when negative ttl expires

   public:

    static CacheEntry * to_CacheEntry(Data & data)
    {
      return reinterpret_cast<CacheEntry *>(Entry::to_Entry(data));
    }

    enum class Status
    {
      AVAILABLE, CALCULATING, READY, FAILED
    };

    static string status_to_string(Status status)
    {
      switch (status)
        {
          case Status::AVAILABLE:
            return "AVAILABLE";
          case Status::CALCULATING:
            return "CALCULATING";
          case Status::READY:
            return "READY";
          case Status::FAILED:
            return "FAILED";
          default:
            return "UNKNOWN";
        }
    }

    CacheEntry() = default;

    CacheEntry(const Key &k)
      : Entry(k)
    {
      // empty
    }

    CacheEntry(Key &&k)
      : Entry(std::move(k))
    {
      // empty
    }

    CacheEntry(Key &&k, Data &&d)
      : Entry(std::move(k))
    {
      this->set_data(std::move(d));
    }

    CacheEntry(const CacheEntry &other)
      : Entry(other), _dlink_lru(other._dlink_lru),
        _status(other._status), _ad_hoc_code(other._ad_hoc_code),
        _positive_ttl_exp_time(other._positive_ttl_exp_time),
        _negative_ttl_exp_time(other._negative_ttl_exp_time)
    {
      // empty
    }

    CacheEntry(CacheEntry &&other) noexcept
      : Entry(std::move(other)), _dlink_lru(std::move(other._dlink_lru)),
        _status(other._status), _ad_hoc_code(other._ad_hoc_code),
        _positive_ttl_exp_time(other._positive_ttl_exp_time),
        _negative_ttl_exp_time(other._negative_ttl_exp_time)
    {
      // empty
    }

    CacheEntry &operator=(const CacheEntry &other)
    {
      if (this == &other)
        return *this;

      Entry::operator=(other);
      _dlink_lru = other._dlink_lru;
      _status = other._status;
      _ad_hoc_code = other._ad_hoc_code;
      _positive_ttl_exp_time = other._positive_ttl_exp_time;
      _negative_ttl_exp_time = other._negative_ttl_exp_time;

      return *this;
    }

    CacheEntry &operator=(CacheEntry &&other) noexcept
    {
      if (this == &other)
        return *this;

      Entry::operator=(std::move(other));
      _dlink_lru = std::move(other._dlink_lru);
      _status = other._status;
      _ad_hoc_code = other._ad_hoc_code;
      _positive_ttl_exp_time = other._positive_ttl_exp_time;
      _negative_ttl_exp_time = other._negative_ttl_exp_time;

      return *this;
    }

    bool operator==(const CacheEntry &other) const
    {
      return this->key() == other.key();
    }

    friend void
    swap(typename Cache<Key, Data, Cmp>::CacheEntry &lhs,
         typename Cache<Key, Data, Cmp>::CacheEntry &rhs) noexcept
    {
      lhs.swap(rhs);
    }

    void swap(CacheEntry &other) noexcept
    {
      this->Entry::swap(other);
      std::swap(this->_dlink_lru, other._dlink_lru);
      std::swap(this->_status, other._status);
      std::swap(this->_ad_hoc_code, other._ad_hoc_code);
      std::swap(this->_positive_ttl_exp_time, other._positive_ttl_exp_time);
      std::swap(this->_negative_ttl_exp_time, other._negative_ttl_exp_time);
    }

    size_t key_hash() const { return key_hash_fct(this->key()); }

    Dlink *link_lru() { return &_dlink_lru; }

    mutex &mtx() { return _mtx; }

    condition_variable &waiting_cv() { return _waiting_cv; }

    Status status() const { return static_cast<Status>(_status); }

    void set_status(const Status &status)
    {
      _status = static_cast<uint8_t>(status);
    }

    int8_t & ad_hoc_code() { return _ad_hoc_code; }

    time_point<high_resolution_clock> positive_ttl_exp_time() const noexcept
    {
      return _positive_ttl_exp_time;
    }

    time_point<high_resolution_clock> negative_ttl_exp_time() const noexcept
    {
      return _negative_ttl_exp_time;
    }

    bool positive_ttl_expired(const high_resolution_clock::time_point &now) const
    {
      return now > positive_ttl_exp_time();
    }

    bool negative_ttl_expired(const high_resolution_clock::time_point &now) const
    {
      return now > negative_ttl_exp_time();
    }

    void set_positive_ttl_exp_time(const time_point<high_resolution_clock> &exp_time)
    {
      _positive_ttl_exp_time = exp_time;
    }

    void set_negative_ttl_exp_time(const time_point<high_resolution_clock> &exp_time)
    {
      _negative_ttl_exp_time = exp_time;
    }

    Data * compress()
    {
      cout << "Compressing data" << endl;
      vector<char> serialized_data;
      if constexpr (std::is_class_v<Data> && requires(Data d) { { d.serialize() } -> std::same_as<vector<char>>; }) {
        serialized_data = this->data()->serialize();
      } else {
        TypeWrapper<Data> data_wrapper(*this->data());
        serialized_data = data_wrapper.serialize();
      }
      Data* calc_data = this->data();
      this->set_data(nullptr);
      this->original_data_size() = serialized_data.size();

      lz4_compress(serialized_data, this->compressed_data());

      assert(this->compressed_data().size() > 0);
      assert(this->data() == nullptr);
      assert(this->original_data_size() > 0);

      return calc_data;
    }

    Data * decompress()
    {
      cout << "Decompressing data" << endl;
      if (this->compressed_data().size() == 0)
        return nullptr;

      if (this->data() == nullptr)
        this->set_data(new Data());

      vector<char> decompressed_data = vector<char>(this->original_data_size());
      lz4_decompress(this->compressed_data(), decompressed_data);
      if constexpr (std::is_class_v<Data> && requires(Data d) { d.deserialize(std::declval<const vector<char>&>()); }) {
        this->data()->deserialize(decompressed_data);
      } else {
        TypeWrapper<Data> data_wrapper;
        data_wrapper.deserialize(decompressed_data);
      }

      this->original_data_size() = 0;
      this->compressed_data().clear();

      assert (this->data() != nullptr);
      assert (this->compressed_data().size() == 0);
      assert (this->original_data_size() == 0);

      return this->data();
    }
  }; // end class CacheEntry

 private:

  struct CacheCmp
  {
    bool operator()(const CacheEntry &lhs, const CacheEntry &rhs) const
    {
      return Cmp()(lhs.key(), rhs.key());
    }
  };

  static size_t cache_entry_hash_fct(const CacheEntry &cache_entry)
  {
    return SuperFastHash((void *) &cache_entry.key(), sizeof(cache_entry.key()));
  }

  // ********** data members of Cache class

  Dlink lru_list; // lru list

  size_t cache_size;  // cache length; MUST less than hash_table.capacity()

  OLhashTable<CacheEntry, Cache::CacheCmp> hash_table;

  seconds positive_ttl;
  seconds negative_ttl;

  mutex mtx; // protects the cache

  bool _compression = false;

 protected:

  LINKNAME_TO_TYPE(CacheEntry, _dlink_lru);

  void insert_entry_to_lru_list(CacheEntry *cache_entry)
  {
    lru_list.insert(cache_entry->link_lru());
  }

  void remove_entry_from_lru_list(CacheEntry *cache_entry)
  {
    cache_entry->link_lru()->del();
  }

  void move_to_lru_front(CacheEntry *cache_entry)
  {
    cache_entry->link_lru()->del();
    lru_list.insert(cache_entry->link_lru());
  }

  void move_to_lru_rear(CacheEntry *cache_entry)
  {
    cache_entry->link_lru()->del();
    lru_list.append(cache_entry->link_lru());
  }

  void do_mru(CacheEntry *cache_entry)
  {
    move_to_lru_front(cache_entry);
  }

  void do_lru(CacheEntry *cache_entry)
  {
    move_to_lru_rear(cache_entry);
  }

  /*
    removes from hash table and makes the entry the least recently used
  */
  void remove_entry_from_hash_table(CacheEntry *cache_entry)
  {
    cache_entry->link_lru()->del();

    hash_table.remove(*cache_entry);
  }

  /* searches and returns the next entry according to lru priority that does not have
     a lock, removes from the hash table and makes the entry the most
     recently used.
  */
  CacheEntry *get_lru_entry()
  {
    assert(hash_table.size() <= cache_size);
    assert(not lru_list.is_empty());

    Dlink *lru_entry_link = lru_list.get_prev();
    CacheEntry *cache_entry = _dlink_lru_to_CacheEntry(lru_entry_link);

    return cache_entry;
  }

  CacheEntry *get_mru_entry()
  {
    assert(hash_table.size() <= cache_size);
    assert(not lru_list.is_empty());

    Dlink *mru_entry_link = lru_list.get_next();
    CacheEntry *cache_entry = _dlink_lru_to_CacheEntry(mru_entry_link);

    return cache_entry;
  }

  bool & compression() { return _compression; }

  CacheEntry *
  insert_in_hash_table(CacheEntry &&entry,
                       const high_resolution_clock::time_point &time_now)
  {
    if (hash_table.size() == cache_size) // Is the cache full?
      { // Yes ==> remove the lru list and from the hash table
        CacheEntry *lru_entry = get_lru_entry();
        remove_entry_from_hash_table(lru_entry);
      }

    assert(hash_table.size() < cache_size);

    // CacheEntry *cache_entry = hash_table.insert(CacheEntry(entry));
    // auto p = hash_table.contains_or_insert(std::move(entry));

    CacheEntry *cache_entry = static_cast<CacheEntry *>(hash_table.search(entry));
    // const bool is_in_table = p.second;

    if (cache_entry != nullptr)
    {
      do_mru(cache_entry);
      mtx.unlock();
      unique_lock lock(cache_entry->mtx());
      cache_entry->waiting_cv().wait(lock, [cache_entry] 
      { 
        return cache_entry->status() != CacheEntry::Status::CALCULATING; 
      });
      return cache_entry;
    }      

    cache_entry = hash_table.insert(std::move(entry));

    assert(cache_entry != nullptr);

    insert_entry_to_lru_list(cache_entry);

    cache_entry->set_positive_ttl_exp_time(time_now + positive_ttl);
    cache_entry->set_negative_ttl_exp_time(time_now + negative_ttl);
    cache_entry->set_status(CacheEntry::Status::READY);

    return cache_entry;
  }

  bool verify_entry_ttl_expired(CacheEntry *cache_entry,
                                const high_resolution_clock::time_point &time_now)
  {
    switch (cache_entry->status())
      {
        case CacheEntry::Status::READY:
          if (cache_entry->positive_ttl_expired(time_now))
            {
              remove_entry_from_hash_table(cache_entry);
              return false;
            }
        break;

        case CacheEntry::Status::FAILED:
          if (cache_entry->negative_ttl_expired(time_now))
            {
              remove_entry_from_hash_table(cache_entry);
              return false;
            }
        break;

        case CacheEntry::Status::CALCULATING:
        case CacheEntry::Status::AVAILABLE:
          ah_fatal_error()
            << "verify_entry_ttl_expired() helper called on an entry with status"
            << CacheEntry::status_to_string(cache_entry->status()) << endl;
        break;

        default:
          return false;
      }

    return true;
  }

  public:

  using MissHandlerType = std::function<bool(const Key &, Data *, int8_t &, Args...)>;

  static constexpr float HashTableRatio = 1.3f;

  MissHandlerType miss_handler;

  Cache(size_t size,
        const seconds &positive_ttl,
        const seconds &negative_ttl,
        MissHandlerType miss_handler,
        bool compression = false )
    : cache_size(size),
      hash_table(static_cast<size_t>(HashTableRatio * size),
                 cache_entry_hash_fct,
                 CacheCmp(),
                 hash_default_lower_alpha,
                 hash_default_upper_alpha,
                 false),
      positive_ttl(positive_ttl), negative_ttl(negative_ttl),
      miss_handler(miss_handler), _compression(compression)
  {
    assert(size > 1);
  }

  // Insert a pair <key, data> into the cache. If successful, it returns a pointer
  // to the data in the cache. Otherwise, it returns nullptr.
  // TODO: may return cache entry directly
  bool insert(Key &&key, Data &&data)
  {
    assert(hash_table.size() <= cache_size);

    CacheEntry entry(std::move(key), std::move(data));

    scoped_lock lock(mtx);

    auto cache_entry = insert_in_hash_table(std::move(entry),
                                            high_resolution_clock::now());

    // if (cache_entry == nullptr)
    //   return nullptr;

    // return cache_entry->data();

    return cache_entry != nullptr;
  }

  bool has(const Key &key)
  {
    assert(hash_table.size() <= cache_size);

    const CacheEntry entry(key);

    scoped_lock lock(mtx);

    auto cache_entry = hash_table.search(entry);

    if (cache_entry == nullptr)
      return false;

    scoped_lock entry_lock(cache_entry->mtx());
    return verify_entry_ttl_expired(cache_entry, high_resolution_clock::now());
  }

  bool touch(const Key &key)
  {
    assert(hash_table.size() <= cache_size);

    const CacheEntry entry(key);

    scoped_lock lock(mtx);

    auto *cache_entry = static_cast<CacheEntry *>(hash_table.search(key));

    if (cache_entry == nullptr)
      return false;

    scoped_lock entry_lock(cache_entry->mtx());
    if (verify_entry_ttl_expired(cache_entry, high_resolution_clock::now()))
      {
        do_mru(cache_entry);
        return true;
      }
    return false;
  };

 private:

  pair<Key, Data>
  get_extreme_from_lrl_list(CacheEntry *(Cache<Key, Data, Cmp>::*get_entry)())
  {
    assert(hash_table.size() <= cache_size);

    ah_domain_error_if(lru_list.is_empty())
        << "get_extreme_from_lrl_list() helper called on an empty lru list";

    scoped_lock lock(mtx);

    auto *cache_entry = (this->*get_entry)();

    return make_pair(cache_entry->key(), *cache_entry->data());
  }

 public:

  pair<Key, Data> get_lru()
  {
    return get_extreme_from_lrl_list(&Cache<Key, Data, Cmp>::get_lru_entry);
  }

  pair<Key, Data> get_mru()
  {
    return get_extreme_from_lrl_list(&Cache<Key, Data, Cmp>::get_mru_entry);
  }

 private:

  pair<Data*, int8_t>
  resolve_cache_miss(CacheEntry *& cache_entry,
                     const high_resolution_clock::time_point &time_now)
  {
    { // allocate a new entry in the hash table for key
      scoped_lock lock(mtx);
      cache_entry = insert_in_hash_table(move(*cache_entry), time_now);
    }

    scoped_lock entry_lock(cache_entry->mtx());
    cache_entry->set_status(CacheEntry::Status::CALCULATING);

    if (cache_entry->data() == nullptr)
      cache_entry->set_data(Data());

    auto status = cache_entry->status();
    if (miss_handler(cache_entry->key(), cache_entry->data(),
                     cache_entry->ad_hoc_code()))
      status = CacheEntry::Status::READY;
    else
      status = CacheEntry::Status::FAILED;

    Data * data = cache_entry->data();
    if (compression())
      data = cache_entry->compress();

    cache_entry->set_status(status);

    cache_entry->waiting_cv().notify_all(); // wake up waiting threads associated with key

    return make_pair(data, cache_entry->ad_hoc_code());
  }

 public:

  // computed/retrieved data, ad hoc status set by the miss handler
  pair<Data *, int8_t> retrieve_from_cache_or_compute(const Key &key)
  {
    assert(hash_table.size() <= cache_size);

    CacheEntry entry(key);
    CacheEntry *cache_entry = nullptr;

    { // 1. Search for the entry in the hash table
      scoped_lock lock(mtx);
      cache_entry = static_cast<CacheEntry *>(hash_table.search(entry));
    }

    if (cache_entry != nullptr)
      { // 2. found!
        unique_lock entry_lock(cache_entry->mtx());
        pair<Data *, int8_t> ret = {nullptr, cache_entry->ad_hoc_code()};
        switch (cache_entry->status()) // check the status of the entry
          {
            case CacheEntry::Status::AVAILABLE: // should not happen
              ah_fatal_error()
                << "retrieve_from_cache_or_compute() helper called on an entry with status"
                << CacheEntry::status_to_string(cache_entry->status()) << endl;
            break;
            case CacheEntry::Status::CALCULATING:
              cache_entry->waiting_cv().wait(entry_lock); // wait for the data to be ready
              assert(cache_entry->status() == CacheEntry::Status::READY or
                     cache_entry->status() == CacheEntry::Status::FAILED);           ret.second = cache_entry->ad_hoc_code();
            case CacheEntry::Status::READY:
              if(compression())
                cache_entry->decompress();
              ret.first = cache_entry->data();
            case CacheEntry::Status::FAILED:
              if (auto time_now = high_resolution_clock::now();
                verify_entry_ttl_expired(cache_entry, time_now))
                do_mru(cache_entry);
              else
                ret = resolve_cache_miss(cache_entry, time_now);
            break;
            default:
              ah_fatal_error()
                << "retrieve_from_cache_or_compute() helper called on an entry with status"
                << static_cast<int>(cache_entry->status()) << endl;
            break;
          }
        return ret;
      }

    // 3. If not found ==> compute the data and insert it into the cache
    cache_entry = &entry;
    return resolve_cache_miss(cache_entry, high_resolution_clock::now());
  }

  void remove(const Key &key)
  {
    scoped_lock lock(mtx);

    CacheEntry entry(key);

    auto *cache_entry = static_cast<CacheEntry *>(hash_table.search(entry));

    if (cache_entry != nullptr)
      remove_entry_from_hash_table(cache_entry);
  }

  const size_t &capacity() const { return cache_size; }

  size_t size() const { return hash_table.size(); }

  const size_t &get_num_busy_slots() const
  {
    return hash_table.get_num_busy_slots();
  }

  // Iterator to traverse the cache. It is not thread-safe.
  struct Iterator : public OLhashTable<CacheEntry, CacheCmp>::Iterator
  {
    Iterator(Cache & _cache)
    : OLhashTable<CacheEntry, CacheCmp>::Iterator(_cache.hash_table)
    {
      // empty
    }

    pair<const Key&, Data*> get_curr()
    {
      CacheEntry & cache_entry =
        OLhashTable<CacheEntry, CacheCmp>::Iterator::get_curr();

      return make_pair(cache_entry.key(), cache_entry.data());
    }
  };

  Iterator get_it()
  {
    return Iterator(*this);
  }

  // Mutex to protect the cache. It could be necessary to protect the cache if
  // the user wants to use the iterator. Use it at your own risk.
  mutex &get_mtx() { return mtx; }
};

#endif // CPP_CACHE_CACHE_H
