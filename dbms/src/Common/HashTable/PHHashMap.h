#include <Common/HashTable/HashMap.h>
#include <parallel_hashmap/phmap.h>

namespace DB
{
template <typename TKey, typename TMapped, typename Hasher>
class PHHashMap
{
public:
    PHHashMap() = default;

    using Key = TKey;
    using Mapped = TMapped;
    using key_type = Key;
    using mapped_type = Mapped;

    using Cell = HashMapCell<Key, Mapped, Hasher>;
    using value_type = typename Cell::value_type;
    using LookupResult = Cell *;

    template <typename KeyHolder>
    void emplace(KeyHolder && key_holder, LookupResult & it, bool & inserted)
    {
        // todo this KeyHolder only works for some situation, like key is Decimal128.
        Key key = key_holder;
        inserted = false;
        auto ph_it = map.lazy_emplace(key, [&](const auto & ctor) {
            Cell cell(key, HashTableNoState());
            ctor(key, cell);
            inserted = true;
        });
        it = &(ph_it->second);
    }

    LookupResult find(const Key & key) const
    {
        auto ph_it = map.find(key);
        // todo check not found
        return &(ph_it->second);
    }

    size_t hash(const Key & key) const
    {
        return Hasher::operator()(key);
    }

    template <typename Func>
    void forEachValue(Func && func)
    {
        for (auto & pair : map)
            func(pair.second.getKey(), pair.second.getMapped());
    }

    template <typename Func>
    void forEachMapped(Func && func)
    {
        for (auto & pair : map)
            func(pair.second.getMapped());
    }

    size_t size() const
    {
        return map.size();
    }

    size_t getBufferSizeInBytes() const
    {
        // todo?
        return map.size();
    }

    bool empty() const { return map.empty(); }

    void clearAndShrink() { map.clear(); }

    template <typename Func>
    void ALWAYS_INLINE mergeToViaEmplace(PHHashMap &, Func &&)
    {
        // todo impl
    }
private:
    phmap::flat_hash_map<Key, Cell, Hasher> map;
};
} // namespace DB
