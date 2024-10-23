#include <Common/phmap/phmap.h>
#include <Common/HashTable/HashTable.h>

template <typename KeyType, typename Mapped, typename Hash>
class PhHashTable : public phmap::flat_hash_map<KeyType, Mapped, Hash>
{
public:
    static constexpr bool isPhMap = true;

    using Self = PhHashTable;
    using Base = phmap::flat_hash_map<KeyType, Mapped, Hash>;
    using Cell = typename Base::slot_type;
    using cell_type = Cell;
    using Key = typename Base::key_type;
    using mapped_type = Mapped;

    using LookupResult = Cell *;
    using ConstLookupResult = const Cell *;

    using typename Base::key_type;
    using typename Base::value_type;

    using Base::prefetch;
    using Base::begin;
    using Base::end;
    using Base::empty;
    using Base::size;
    using Base::capacity;
    using Base::clear;
    using Base::slot_at;
    using Base::find_or_prepare_insert;
    using Base::find_impl;
    using Base::hash;
    using Base::lazy_emplace;
    using Base::lazy_emplace_with_hash;

    ALWAYS_INLINE inline size_t getHash(const KeyType & key) const
    {
        return hash(key);
    }

    template <typename KeyHolder>
    ALWAYS_INLINE inline void emplace(KeyHolder && key_holder, LookupResult & it, bool & inserted)
    {
        const auto & key = keyHolderGetKey(key_holder);
        auto iter = lazy_emplace(key, [&](const auto & ctor) { // TODO init inserted as false
            inserted = true;
            ctor(key, nullptr);
            keyHolderPersistKey(key_holder);
        });
        it = iter.getPtr();
        if (!inserted)
            keyHolderDiscardKey(key_holder);
    }

    template <typename KeyHolder>
    ALWAYS_INLINE inline void emplace(KeyHolder && key_holder, LookupResult & it, bool & inserted, size_t hashval)
    {
        const auto & key = keyHolderGetKey(key_holder);
        auto iter = lazy_emplace_with_hash(key, hashval, [&](const auto & ctor) {
            inserted = true;
            ctor(key, nullptr);
            keyHolderPersistKey(key_holder);
        });
        it = iter.getPtr();
        if (!inserted)
            keyHolderDiscardKey(key_holder);
    }

    ALWAYS_INLINE inline LookupResult find(const KeyType & key, size_t hashval)
    {
        size_t offset;
        if (find_impl(key, hashval, offset))
            return slot_at(offset);
        else
            return nullptr;
    }

    ALWAYS_INLINE inline LookupResult find(const KeyType & key)
    {
        const auto hashval = this->hash(key);
        find(key, hashval);
    }

    ALWAYS_INLINE inline ConstLookupResult find(const KeyType & key, size_t hashval) const
    {
        return const_cast<std::decay_t<decltype(*this)> *>(this)->find(key, hashval);
    }

    ALWAYS_INLINE inline ConstLookupResult find(const KeyType & key) const
    {
        return const_cast<std::decay_t<decltype(*this)> *>(this)->find(key);
    }

    template <typename Func>
    void forEachValue(Func && func)
    {
        for (auto iter = begin(); iter != end(); ++iter)
        {
            func(iter->first, iter->second);
        }
    }

    template <typename Func>
    void forEachMapped(Func && func)
    {
        for (auto iter = begin(); iter != end(); ++iter)
        {
            func(iter->second);
        }
    }

    ALWAYS_INLINE inline typename Base::mapped_type & operator[](const Key & key)
    {
        LookupResult it = nullptr;
        bool inserted = false;
        emplace(key, it, inserted);

        if (inserted)
            new (&it->getMapped()) (typename Base::mapped_type)();

        return it->getMapped();
    }

    ALWAYS_INLINE inline size_t getBufferSizeInBytes() const
    {
        // TODO correctness for ctro?
        return capacity() * (sizeof(typename Base::slot_type) + sizeof(typename phmap::priv::ctrl_t));
    }

    ALWAYS_INLINE inline size_t getBufferSizeInCells() const
    {
        // TODO correctness for ctro?
        return capacity();
    }

    ALWAYS_INLINE inline void clearAndShrink()
    {
        clear();
    }

    void write(DB::WriteBuffer & ) const
    {
        // DB::writeBinary(value.first, wb);
        // DB::writeBinary(value.second, wb);
    }

    void writeText(DB::WriteBuffer & ) const
    {
        // DB::writeDoubleQuoted(value.first, wb);
        // DB::writeChar(',', wb);
        // DB::writeDoubleQuoted(value.second, wb);
    }

    /// Deserialization, in binary and text form.
    void read(DB::ReadBuffer & )
    {
        // DB::readBinary(value.first, rb);
        // DB::readBinary(value.second, rb);
    }

    void readText(DB::ReadBuffer & )
    {
        // TODO
        // DB::readDoubleQuoted(value.first, rb);
        // DB::assertChar(',', rb);
        // DB::readDoubleQuoted(value.second, rb);
    }
    // TODO insertUniqueNonZero()
    // TODO lazy_emplace_with_hash
    void setResizeCallback(const ResizeCallback &)
    {
        // TODO
    }

    template <typename Func>
    ALWAYS_INLINE inline void mergeToViaEmplace(Self & that, Func && func)
    {
        for (auto it = this->begin(), end = this->end(); it != end; ++it)
        {
            typename Self::LookupResult res_it;
            bool inserted;
            that.emplace(it->first, res_it, inserted);
            func(res_it->getMapped(), it->second, inserted);
        }
    }

    template <typename Func>
    ALWAYS_INLINE inline void mergeToViaFind(Self & that, Func && func)
    {
        for (auto it = this->begin(), end = this->end(); it != end; ++it)
        {
            auto res_it = that.find(it->first);
            if (!res_it)
                func(it->second, it->second, false);
            else
                func(res_it->getMapped(), it->second, true);
        }
    }
};
