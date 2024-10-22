#include <Common/phmap/phmap.h>
#include <Common/HashTable/HashTable.h>

template <typename KeyType, typename Mapped, typename Hash>
class PhHashTable : public phmap::flat_hash_map<KeyType, Mapped, Hash>
{
public:
    static constexpr bool isPhMap = true;

    using Base = phmap::flat_hash_map<KeyType, Mapped, Hash>;
    using Cell = typename Base::slot_type;
    using cell_type = Cell;
    using Key = typename Base::key_type;
    using mapped_type = Mapped;

    using LookupResult = Cell *;
    using ConstLookupResult = const Cell *;

    using Base::key_type;
    using Base::value_type;

    using Base::prefetch;
    using Base::begin;
    using Base::end;
    using Base::empty;
    using Base::size;
    using Base::capacity;
    using Base::clear;

    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder && key_holder, LookupResult & it, bool & inserted)
    {
        const auto & key = keyHolderGetKey(key_holder);
        auto res = this->find_or_prepare_insert(key);
        it = Base::slots_ + res.first;
        inserted = res.second;
    }

    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder && key_holder, size_t hashval, LookupResult & it, bool & inserted)
    {
        const auto & key = keyHolderGetKey(key_holder);
        auto res = this->find_or_prepare_insert(key, hashval);
        it = Base::slots_ + res.first;
        inserted = res.second;
    }

    LookupResult ALWAYS_INLINE find(const KeyType & key, size_t hashval) const
    {
        size_t offset;
        if (this->find_impl(key, hashval, offset))
            // TODO maybe a func in BaseClass
            return Base::slots_ + offset;
        else
            return nullptr;
    }

    LookupResult ALWAYS_INLINE find(const KeyType & key) const
    {
        const auto hashval = this->hash(key);
        find(key, hashval);
    }

    template <typename Func>
    void forEachValue(Func && func)
    {
        for (auto iter = begin(); iter != end(); ++iter)
        {
            // TODO key is const,
            // check map_slot_type
            func(iter->first, iter->second);
        }
    }

    template <typename Func>
    void foreachMapped(Func && func)
    {
        for (auto iter = begin(); iter != end(); ++iter)
        {
            func(iter->second);
        }
    }

    typename Base::mapped_type & ALWAYS_INLINE operator[](const Key & key)
    {
        LookupResult it = nullptr;
        bool inserted = false;
        emplace(key, it, inserted);

        if (inserted)
            new (&it->getMapped()) (typename Base::mapped_type)();

        return it->getMapped();
    }

    size_t ALWAYS_INLINE getBufferSizeInBytes() const
    {
        return capacity() * (sizeof(Base::slot_type) + sizeof(phmap::priv::ctrl_t));
    }

    size_t ALWAYS_INLINE getBufferSizeInCells() const
    {
        return capacity();
    }

    void ALWAYS_INLINE clearAndShrink()
    {
        clear();
    }

    // void read()
    // void readText()
    // void write()
    // void writeText()
    // TODO insertUniqueNonZero()
    // TODO lazy_emplace_with_hash
    void setResizeCallback(const ResizeCallback &)
    {
        // TODO
    }
};
