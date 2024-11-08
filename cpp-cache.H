//
// Created by lrleon on 11/4/24.
//

#ifndef CPP_CACHE__CPP_CACHE_H
#define CPP_CACHE__CPP_CACHE_H

# include <tpl_olhash.H>

// TODO: we'll use the ctor with resize set to false and a fix size around 30% more
// cells than the requested size. This is to avoid resizing the table qnd favoring
// the hash entropy


# include <memory>
# include <aleph.H>
# include <tpl_dnode.H>
//# include <tpl_lhash.H>


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
  
  class CacheEntry //: public LhashTable<Key>::Bucket
  {
    friend class Cache<Key, Data>;
    
    Data    data;
    Dlink   dlink_lru; // enlace cola lru
    Dlink   dlink_inside;
    
    bool locked; // Indicates that entry cannot be deleted
    
    bool is_in_hash_table;
    
    void lock()
    {
      if (locked)
        throw std::runtime_error("CacheEntry is already locked");
      
      locked = true;
    }
    
    void unlock()
    {
      if (not locked)
        throw std::runtime_error("CacheEntry is not locked");
      
      locked = false;
    }
    
    Dlink* link_lru() { return &dlink_lru; }
    
    Dlink* link_inside() { return &dlink_inside; }
  
  public:
    
    CacheEntry(const Key & k, const Data & d)
      : /* LhashTable<Key>::Bucket(k) */ data(d), locked(false),
        is_in_hash_table(false)
    {
      // empty
    }
    
    CacheEntry() : data(), locked(false), is_in_hash_table(false)
    {
      // empty
    }
    
    Data & get_data() { return data; }
    
    const bool & is_locked() const { return locked; }
    
    const bool & is_in_table() const { return is_in_hash_table; }
    
    static CacheEntry * convert_to_cache_entry(Data * data_ptr)
    {
      return reinterpret_cast<CacheEntry*>(data_ptr);
    }
  }; // end class CacheEntry

private:
  
  Dlink           lru_list;
  Dlink           locked_list;
  Dlink           inside_list;
  
  //LhashTable<Key> hash_table;
  
  size_t          cache_size;  /* longitud del cache */

public:
  
  const size_t & get_num_entries() const
  {
    assert(hash_table.size() <= cache_size);
    
    return hash_table.size();
  }

private:
  
  size_t   num_lru;     /* number of elements in a lru list*/
  size_t   num_locked;  /* number of blocked elements */
  /* This is a list of memory chunks. Each one is added to the list
     while required during the cache lifetime.

     A chunk of memory has the interesting property of being continuous
     in memory, therefore, can be cached by the computer.
  */
  typedef Dnode<CacheEntry*> Chunk_Descriptor;
  
  Chunk_Descriptor chunk_list;

protected:
  
  LINKNAME_TO_TYPE(CacheEntry, dlink_lru);
  
  LINKNAME_TO_TYPE(CacheEntry, dlink_inside);
  
  void insert_entry_to_lru_list(CacheEntry * cache_entry)
  {
    num_lru++;
    lru_list.insert(cache_entry->link_lru());
  }
  
  void remove_entry_from_lru_list(CacheEntry * cache_entry)
  {
    num_lru--;
    cache_entry->link_lru()->del();
  }
  
  void insert_entry_to_locked_list(CacheEntry * cache_entry)
  {
    num_locked++;
    locked_list.insert(cache_entry->link_lru());
  }
  
  void remove_entry_from_locked_list(CacheEntry * cache_entry)
  {
    num_locked--;
    cache_entry->link_lru()->del();
  }
  
  void move_to_inside_front(CacheEntry * cache_entry)
  {
    cache_entry->link_inside()->del();
    inside_list.insert(cache_entry->link_inside());
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
    cache_entry->is_in_hash_table = false;
    do_lru(cache_entry);
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
    
    assert(not cache_entry->is_locked());
    
    if (cache_entry->is_in_hash_table)
    {
      assert(hash_table.search(cache_entry->get_key()) == cache_entry);
      remove_entry_from_hash_table(cache_entry);
    }
    
    do_mru(cache_entry);
    
    return cache_entry;
  }
  
  CacheEntry * insert_pair(const Key & key, const Data & data)
  {
    CacheEntry *cache_entry = get_lru_entry();
    
    cache_entry->get_key()  = key;
    cache_entry->get_data() = data;
    
    inside_list.insert(cache_entry->link_inside());
    
    hash_table.insert(cache_entry);
    cache_entry->is_in_hash_table = true;
    
    return cache_entry;
  }

public:
  
  Cache(size_t (*hash_fct)(const Key&), const size_t & size)
    : hash_table(size, hash_fct),
      cache_size(hash_table.capacity()), num_lru(0), num_locked(0)
  {
    assert(size > 1);
    assert(hash_fct != nullptr);
    
    auto * entries_array = new CacheEntry [cache_size];
    
    try
    {
      std::unique_ptr<Chunk_Descriptor>
        chunk_descriptor (new Chunk_Descriptor (entries_array));
      
      chunk_list.insert(chunk_descriptor.get());
      
      for (int i = 0; i < cache_size; i++)
        insert_entry_to_lru_list(&entries_array[i]);
      
      chunk_descriptor.release();
    }
    catch (...)
    {
      delete [] entries_array;
      throw;
    }
  }
  
  virtual ~Cache()
  {
    assert(hash_table.size() <= cache_size);
    
    while (not chunk_list.is_empty())
    {
      Chunk_Descriptor * current_chunk = chunk_list.remove_next();
      
      delete [] current_chunk->get_data();
      delete current_chunk;
    }
  }
  
  CacheEntry * search(const Key & key)
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
  
  /* returns the next cache entry with the same key
     cacheEntry if it exists or null otherwise */
  CacheEntry * search_next(CacheEntry * cache_entry)
  {
    auto *next_entry =
      static_cast<CacheEntry*>(hash_table.search_next(cache_entry));
    
    if (next_entry != nullptr)
    {
      do_mru(next_entry);
      move_to_inside_front(cache_entry);
    }
    
    return next_entry;
  }
  
  CacheEntry * insert(const Key& key, const Data& data)
  {
    assert(hash_table.size() <= cache_size);
    
    return insert_pair(key, data);
  }
  
  void lock_entry(CacheEntry * cache_entry)
  {
    assert(num_locked < get_num_entries());
    assert(num_lru > 0);
    assert(hash_table.search(cache_entry->get_key()) == cache_entry);
    assert(cache_entry->is_in_hash_table);
    
    if (cache_entry->is_locked())
      throw std::runtime_error("CacheEntry is already locked");
    
    remove_entry_from_lru_list(cache_entry);
    insert_entry_to_locked_list(cache_entry);
    
    cache_entry->lock();
  }
  
  void unlock_entry(CacheEntry * cache_entry)
  {
    assert(hash_table.search(cache_entry->get_key()) == cache_entry);
    assert(cache_entry->is_in_hash_table);
    assert(num_locked <= get_num_entries());
    
    if (not cache_entry->is_locked())
      throw std::runtime_error("CacheEntry is not locked");
    
    remove_entry_from_locked_list(cache_entry);
    insert_entry_to_lru_list(cache_entry);
    
    cache_entry->unlock();
  }
  
  void remove(CacheEntry * cache_entry)
  {
    assert(hash_table.search(cache_entry->get_key()) == cache_entry);
    
    if (cache_entry->is_locked())
      throw std::runtime_error("CacheEntry is already locked");
    
    remove_entry_from_hash_table(cache_entry);
  }
  
  void expand(const size_t & plus_size)
  {
    assert(hash_table.size() <= cache_size);
    
    if (plus_size == 0)
      throw std::range_error ("bad plus_size");
    
    const size_t new_cache_size = cache_size + plus_size;
    
    auto * entries_array = new CacheEntry [plus_size];
    
    try
    {
      std::unique_ptr<Chunk_Descriptor>
        chunk_descriptor (new Chunk_Descriptor (entries_array));
      
      hash_table.resize(13*(new_cache_size)/10);
      
      for (int i = 0; i < plus_size; i++)
        insert_entry_to_lru_list(&entries_array[i]);
      
      chunk_list.insert(chunk_descriptor.release());
      
      cache_size = new_cache_size;
    }
    catch (...)
    {
      delete []  entries_array;
      throw;
    }
  }
  
  const size_t & capacity() const { return cache_size; }
  
  const size_t & size() const { return hash_table.size(); }
  
  const size_t & get_num_locked() const { return num_locked; }
  
  const size_t & get_num_busy_slots() const
  {
    return hash_table.get_num_busy_slots();
  }
  
  struct Iterator : public Dlink::Iterator
  {
    Iterator(Cache& _cache) : Dlink::Iterator(&_cache.inside_list)
    {
      // empty
    }
    
    CacheEntry * get_curr()
    {
      CacheEntry * ret_val =
        CacheEntry::dlink_inside_to_Cache_Entry(Dlink::Iterator::get_curr());
      
      assert(ret_val->is_in_table());
      
      return ret_val;
    }
  };
};



#endif //CPP_CACHE__CPP_CACHE_H
