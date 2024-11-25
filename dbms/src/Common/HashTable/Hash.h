// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Common/Decimal.h>
#include <Common/phmap/phmap_bits.h>
#include <Core/Types.h>
#include <city.h>
#include <common/StringRef.h>
#include <common/types.h>
#include <common/unaligned.h>

#include <boost/functional/hash/hash.hpp>
#include <type_traits>


/** Hash functions that are better than the trivial function std::hash.
  *
  * Example: when we do aggregation by the visitor ID, the performance increase is more than 5 times.
  * This is because of following reasons:
  * - in Yandex, visitor identifier is an integer that has timestamp with seconds resolution in lower bits;
  * - in typical implementation of standard library, hash function for integers is trivial and just use lower bits;
  * - traffic is non-uniformly distributed across a day;
  * - we are using open-addressing linear probing hash tables that are most critical to hash function quality,
  *   and trivial hash function gives disastrous results.
  */

/** Taken from MurmurHash. This is Murmur finalizer.
  * Faster than intHash32 when inserting into the hash table UInt64 -> UInt64, where the key is the visitor ID.
  */
inline DB::UInt64 intHash64(DB::UInt64 x)
{
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;

    return x;
}

/** CRC32C is not very high-quality as a hash function,
  *  according to avalanche and bit independence tests (see SMHasher software), as well as a small number of bits,
  *  but can behave well when used in hash tables,
  *  due to high speed (latency 3 + 1 clock cycle, throughput 1 clock cycle).
  * Works only with SSE 4.2 support.
  */
#ifdef __SSE4_2__
#include <nmmintrin.h>
#endif

#if defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
#include <arm_acle.h>
#include <arm_neon.h>
#endif

inline DB::UInt64 intHashCRC32(DB::UInt64 x)
{
#ifdef __SSE4_2__
    return _mm_crc32_u64(-1ULL, x);
#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
    return __crc32cd(-1U, x);
#else
    /// On other platforms we do not have CRC32. NOTE This can be confusing.
    return intHash64(x);
#endif
}

inline DB::UInt64 intHashCRC32(DB::UInt64 x, DB::UInt64 updated_value)
{
#ifdef __SSE4_2__
    return _mm_crc32_u64(updated_value, x);
#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
    return __crc32cd(updated_value, x);
#else
    /// On other platforms we do not have CRC32. NOTE This can be confusing.
    return intHash64(x) ^ updated_value;
#endif
}

template <typename T>
inline DB::UInt64 wideIntHashCRC32(const T & x, DB::UInt64 updated_value)
{
    if constexpr (DB::IsDecimal<T>)
    {
        if constexpr (is_fit_register<typename T::NativeType>)
            return intHashCRC32(x.value, updated_value);
        else
            return wideIntHashCRC32(x.value, updated_value);
    }
    else if constexpr (is_boost_number_v<T>)
    {
        auto backend_value = x.backend();
        for (size_t i = 0; i < backend_value.size(); ++i)
        {
            updated_value = intHashCRC32(backend_value.limbs()[i], updated_value);
        }
        return intHashCRC32(backend_value.sign(), updated_value);
    }
    else if constexpr (std::is_same_v<T, DB::UInt128>)
    {
        updated_value = intHashCRC32(x.low, updated_value);
        updated_value = intHashCRC32(x.high, updated_value);
        return updated_value;
    }
    else if constexpr (std::is_same_v<T, DB::Int128>)
    {
        const auto * begin = reinterpret_cast<const DB::UInt64 *>(&x);
        updated_value = intHashCRC32(unalignedLoad<DB::UInt64>(begin), updated_value);
        updated_value = intHashCRC32(unalignedLoad<DB::UInt64>(begin + 1), updated_value);
        return updated_value;
    }
    else if constexpr (std::is_same_v<T, DB::UInt256>)
    {
        updated_value = intHashCRC32(x.a, updated_value);
        updated_value = intHashCRC32(x.b, updated_value);
        updated_value = intHashCRC32(x.c, updated_value);
        updated_value = intHashCRC32(x.d, updated_value);
        return updated_value;
    }
    static_assert(
        DB::IsDecimal<T> || is_boost_number_v<T> || std::is_same_v<T, DB::UInt128> || std::is_same_v<T, DB::Int128>
        || std::is_same_v<T, DB::UInt256>);
    __builtin_unreachable();
}

template <typename T>
inline DB::UInt64 wideIntHashCRC32(const T & x)
{
    return wideIntHashCRC32(x, -1);
}

inline UInt32 updateWeakHash32(const DB::UInt8 * pos, size_t size, DB::UInt32 updated_value)
{
    if (size < 8)
    {
        UInt64 value = 0;

        switch (size)
        {
        case 0:
            break;
        case 1:
            __builtin_memcpy(&value, pos, 1);
            break;
        case 2:
            __builtin_memcpy(&value, pos, 2);
            break;
        case 3:
            __builtin_memcpy(&value, pos, 3);
            break;
        case 4:
            __builtin_memcpy(&value, pos, 4);
            break;
        case 5:
            __builtin_memcpy(&value, pos, 5);
            break;
        case 6:
            __builtin_memcpy(&value, pos, 6);
            break;
        case 7:
            __builtin_memcpy(&value, pos, 7);
            break;
        default:
            __builtin_unreachable();
        }

        reinterpret_cast<unsigned char *>(&value)[7] = size;
        return intHashCRC32(value, updated_value);
    }

    const auto * end = pos + size;
    while (pos + 8 <= end)
    {
        auto word = unalignedLoad<UInt64>(pos);
        updated_value = intHashCRC32(word, updated_value);

        pos += 8;
    }

    if (pos < end)
    {
        /// If string size is not divisible by 8.
        /// Lets' assume the string was 'abcdefghXYZ', so it's tail is 'XYZ'.
        DB::UInt8 tail_size = end - pos;
        /// Load tailing 8 bytes. Word is 'defghXYZ'.
        auto word = unalignedLoad<UInt64>(end - 8);
        /// Prepare mask which will set other 5 bytes to 0. It is 0xFFFFFFFFFFFFFFFF << 5 = 0xFFFFFF0000000000.
        /// word & mask = '\0\0\0\0\0XYZ' (bytes are reversed because of little ending)
        word &= (~UInt64(0)) << DB::UInt8(8 * (8 - tail_size));
        /// Use least byte to store tail length.
        word |= tail_size;
        /// Now word is '\3\0\0\0\0XYZ'
        updated_value = intHashCRC32(word, updated_value);
    }

    return updated_value;
}

template <typename T>
inline size_t defaultHash64(std::enable_if_t<is_fit_register<T>, T> key)
{
    union
    {
        T in;
        DB::UInt64 out;
    } u;
    u.out = 0;
    u.in = key;
    return intHash64(u.out);
}


template <typename T>
inline size_t defaultHash64(const std::enable_if_t<!is_fit_register<T>, T> & key)
{
    if constexpr (std::is_same_v<T, DB::UInt128>)
    {
        return CityHash_v1_0_2::Hash128to64({key.low, key.high});
    }
    else if constexpr (std::is_same_v<T, DB::Int128>)
    {
        const auto * begin = reinterpret_cast<const DB::UInt64 *>(&key);
        return CityHash_v1_0_2::Hash128to64({unalignedLoad<DB::UInt64>(begin), unalignedLoad<DB::UInt64>(begin + 1)});
    }
    else if constexpr (std::is_same_v<T, DB::UInt256>)
    {
        return CityHash_v1_0_2::Hash128to64(
            {CityHash_v1_0_2::Hash128to64({key.a, key.b}), CityHash_v1_0_2::Hash128to64({key.c, key.d})});
    }
    else if constexpr (is_boost_number_v<T>)
    {
        return boost::multiprecision::hash_value(key);
    }
    static_assert(
        is_boost_number_v<T> || std::is_same_v<T, DB::UInt128> || std::is_same_v<T, DB::Int128>
        || std::is_same_v<T, DB::UInt256>);
    __builtin_unreachable();
}

template <typename T, typename Enable = void>
struct DefaultHash;

template <typename T>
struct DefaultHash<T, std::enable_if_t<!DB::IsDecimal<T> && is_fit_register<T>, void>>
{
    std::enable_if_t<is_fit_register<T>, size_t> operator()(T key) const { return defaultHash64<T>(key); }
};

template <typename T>
struct DefaultHash<T, std::enable_if_t<!DB::IsDecimal<T> && !is_fit_register<T>, void>>
{
    size_t operator()(const T & key) const { return defaultHash64<T>(key); }
};

template <typename T>
struct DefaultHash<T, std::enable_if_t<DB::IsDecimal<T>>>
{
    size_t operator()(const T & key) const { return defaultHash64<typename T::NativeType>(key.value); }
};

template <>
struct DefaultHash<StringRef> : public StringRefHash
{
};

template <typename T>
inline size_t hashCRC32(std::enable_if_t<is_fit_register<T>, T> key)
{
    union
    {
        T in;
        DB::UInt64 out;
    } u;
    u.out = 0;
    u.in = key;
    return intHashCRC32(u.out);
}

template <typename T>
inline size_t hashCRC32(const std::enable_if_t<!is_fit_register<T>, T> & key)
{
    return wideIntHashCRC32(key);
}

template <typename T>
struct HashCRC32;

#define DEFINE_HASH(T)                     \
    template <>                            \
    struct HashCRC32<T>                    \
    {                                      \
        static_assert(is_fit_register<T>); \
        size_t operator()(T key) const     \
        {                                  \
            return hashCRC32<T>(key);      \
        }                                  \
    };

#define DEFINE_HASH_WIDE(T)                    \
    template <>                                \
    struct HashCRC32<T>                        \
    {                                          \
        static_assert(!is_fit_register<T>);    \
        size_t operator()(const T & key) const \
        {                                      \
            return hashCRC32<T>(key);          \
        }                                      \
    };

DEFINE_HASH(DB::UInt8)
DEFINE_HASH(DB::UInt16)
DEFINE_HASH(DB::UInt32)
DEFINE_HASH(DB::UInt64)
DEFINE_HASH(DB::Int8)
DEFINE_HASH(DB::Int16)
DEFINE_HASH(DB::Int32)
DEFINE_HASH(DB::Int64)
DEFINE_HASH(DB::Float32)
DEFINE_HASH(DB::Float64)

DEFINE_HASH_WIDE(DB::UInt128)
DEFINE_HASH_WIDE(DB::UInt256)
DEFINE_HASH_WIDE(DB::Int128)
DEFINE_HASH_WIDE(DB::Int256)
DEFINE_HASH_WIDE(DB::Int512)

#undef DEFINE_HASH

/// It is reasonable to use for UInt8, UInt16 with sufficient hash table size.
struct TrivialHash
{
    template <typename T, std::enable_if_t<is_fit_register<T>, int> = 0>
    size_t operator()(T key) const
    {
        return key;
    }

    size_t operator()(const UInt128 & key) const { return key.low; }

    size_t operator()(const Int128 & key) const { return key; }

    size_t operator()(const UInt256 & key) const { return key.a; }

    template <typename T, std::enable_if_t<is_boost_number_v<T>, int> = 0>
    size_t operator()(const T & key) const
    {
        return key;
    }
};


/** A relatively good non-cryptographic hash function from UInt64 to UInt32.
  * But worse (both in quality and speed) than just cutting intHash64.
  * Taken from here: http://www.concentric.net/~ttwang/tech/inthash.htm
  *
  * Slightly changed compared to the function by link: shifts to the right are accidentally replaced by a cyclic shift to the right.
  * This change did not affect the smhasher test results.
  *
  * It is recommended to use different salt for different tasks.
  * That was the case that in the database values were sorted by hash (for low-quality pseudo-random spread),
  *  and in another place, in the aggregate function, the same hash was used in the hash table,
  *  as a result, this aggregate function was monstrously slowed due to collisions.
  *
  * NOTE Salting is far from perfect, because it commutes with first steps of calculation.
  *
  * NOTE As mentioned, this function is slower than intHash64.
  * But occasionally, it is faster, when written in a loop and loop is vectorized.
  */
template <DB::UInt64 salt>
inline DB::UInt32 intHash32(DB::UInt64 key)
{
    key ^= salt;

    key = (~key) + (key << 18);
    key = key ^ ((key >> 31) | (key << 33));
    key = key * 21;
    key = key ^ ((key >> 11) | (key << 53));
    key = key + (key << 6);
    key = key ^ ((key >> 22) | (key << 42));

    return key;
}

/// For containers.
template <typename T, DB::UInt64 salt = 0, typename E = void>
struct IntHash32;

template <typename T, DB::UInt64 salt>
struct IntHash32<T, salt, std::enable_if_t<is_fit_register<T>, void>>
{
    size_t operator()(T key) const { return intHash32<salt>(key); }
};

template <typename T, DB::UInt64 salt>
struct IntHash32<T, salt, std::enable_if_t<!is_fit_register<T>, void>>
{
    size_t operator()(const T & key) const
    {
        if constexpr (std::is_same_v<T, DB::UInt128>)
        {
            return intHash32<salt>(key.low ^ key.high);
        }
        else if constexpr (std::is_same_v<T, DB::UInt256>)
        {
            return intHash32<salt>(key.a ^ key.b ^ key.c ^ key.d);
        }
        else
        {
            return intHash32<salt>(defaultHash64(key));
        }
    }
};

enum PhHashSeed
{
    PhHashSeed1,
    PhHashSeed2
};

template <int n, PhHashSeed seed>
class PhHashMixSeed
{
public:
    inline size_t operator()(size_t) const;
};

template <>
class PhHashMixSeed<4, PhHashSeed1>
{
public:
    inline size_t operator()(size_t a) const
    {
        static constexpr uint64_t kmul = 0xcc9e2d51UL;
        uint64_t l = a * kmul;
        return static_cast<size_t>(l ^ (l >> 32u));
    }
};

template <>
class PhHashMixSeed<8, PhHashSeed1>
{
public:
    inline size_t operator()(size_t a) const
    {
        static constexpr uint64_t k = 0xde5fb9d2630458e9ULL;
        uint64_t h;
        uint64_t l = umul128(a, k, &h);
        return static_cast<size_t>(h + l);
    }
};
template <typename T, PhHashSeed seed>
struct PhHash
{
    std::size_t operator()(const T & value) const
    {
        return PhHashMixSeed<sizeof(size_t), seed>()(std::hash<T>()(value));
    }
};

template <typename T>
inline T unaligned_load(const void* p) {
    T res{};
    memcpy(&res, p, sizeof(res));
    return res;
}

inline uint32_t crc_hash_32(const void* data, int32_t bytes, uint32_t hash) {
#if defined(__x86_64__) && !defined(__SSE4_2__)
    return static_cast<uint32_t>(crc32(hash, (const unsigned char*)data, bytes));
#else
    uint32_t words = bytes / sizeof(uint32_t);
    bytes = bytes % 4 /*sizeof(uint32_t)*/;

    auto* p = reinterpret_cast<const uint8_t*>(data);

    while (words--) {
#if defined(__x86_64__)
        hash = _mm_crc32_u32(hash, unaligned_load<uint32_t>(p));
#elif defined(__aarch64__)
        hash = __crc32cw(hash, unaligned_load<uint32_t>(p));
#else
#error "Not supported architecture"
#endif
        p += sizeof(uint32_t);
    }

    while (bytes--) {
#if defined(__x86_64__)
        hash = _mm_crc32_u8(hash, *p);
#elif defined(__aarch64__)
        hash = __crc32cb(hash, *p);
#else
#error "Not supported architecture"
#endif
        ++p;
    }

    // The lower half of the CRC hash has has poor uniformity, so swap the halves
    // for anyone who only uses the first several bits of the hash.
    hash = (hash << 16u) | (hash >> 16u);
    return hash;
#endif
}

inline uint64_t crc_hash_64(const void* data, int32_t length, uint64_t hash) {
#if defined(__x86_64__) && !defined(__SSE4_2__)
    return crc32(hash, (const unsigned char*)data, length);
#else
    if (unlikely (length < 8)) {
        return crc_hash_32(data, length, static_cast<uint32_t>(hash));
    }

    uint64_t words = length / sizeof(uint64_t);
    auto* p = reinterpret_cast<const uint8_t*>(data);
    auto* end = reinterpret_cast<const uint8_t*>(data) + length;
    while (words--) {
#if defined(__x86_64__) && defined(__SSE4_2__)
        hash = _mm_crc32_u64(hash, unaligned_load<uint64_t>(p));
#elif defined(__aarch64__)
        hash = __crc32cd(hash, unaligned_load<uint64_t>(p));
#else
#error "Not supported architecture"
#endif
        p += sizeof(uint64_t);
    }
    // Reduce the branch condition
    p = end - 8;
#if defined(__x86_64__)
    hash = _mm_crc32_u64(hash, unaligned_load<uint64_t>(p));
#elif defined(__aarch64__)
    hash = __crc32cd(hash, unaligned_load<uint64_t>(p));
#else
#error "Not supported architecture"
#endif
    p += sizeof(uint64_t);
    return hash;
#endif
}

// TODO: 0x811C9DC5 is not prime number
static const uint32_t CRC_HASH_SEED1 = 0x811C9DC5;
static const uint32_t CRC_HASH_SEED2 = 0x811C9DD7;

class SliceHash {
public:
    std::size_t operator()(const StringRef& slice) const {
        return crc_hash_64(slice.data, static_cast<int32_t>(slice.size), CRC_HASH_SEED1);
    }
};

template <PhHashSeed>
class SliceHashWithSeed {
public:
    std::size_t operator()(const StringRef& slice) const;
};

template <>
class SliceHashWithSeed<PhHashSeed1> {
public:
    std::size_t operator()(const StringRef& slice) const {
        return crc_hash_64(slice.data, static_cast<int32_t>(slice.size), CRC_HASH_SEED1);
    }
};

template <>
class SliceHashWithSeed<PhHashSeed2> {
public:
    std::size_t operator()(const StringRef& slice) const {
        return crc_hash_64(slice.data, static_cast<int32_t>(slice.size), CRC_HASH_SEED2);
    }
};

// https://github.com/HowardHinnant/hash_append/issues/7
template <typename T>
inline void hash_combine(uint64_t& seed, const T& val) {
    seed ^= std::hash<T>{}(val) + 0x9e3779b97f4a7c15LLU + (seed << 12) + (seed >> 4);
}

inline uint64_t hash_128(uint64_t seed, const Int128 & val) {
    auto low = static_cast<size_t>(val);
    auto high = static_cast<size_t>(val >> 64);
    hash_combine(seed, low);
    hash_combine(seed, high);
    return seed;
}

template <PhHashSeed seed>
struct Hash128WithSeed {
    std::size_t operator()(const Int128 & value) const {
        return PhHashMixSeed<sizeof(size_t), seed>()(hash_128(seed, value));
    }
};

inline uint64_t hash_256(uint64_t seed, const Int256 & val) {
    auto backend_value = val.backend();
    for (size_t i = 0; i < backend_value.size(); ++i)
    {
        hash_combine(seed, backend_value.limbs()[i]);
    }
    return seed;
}

inline uint64_t hash_256(uint64_t seed, const UInt256 & val) {
    hash_combine(seed, val.a);
    hash_combine(seed, val.b);
    hash_combine(seed, val.c);
    hash_combine(seed, val.d);
    return seed;
}

template <PhHashSeed seed>
struct Hash256WithSeed {
    std::size_t operator()(const Int256 & value) const {
        return PhHashMixSeed<sizeof(size_t), seed>()(hash_256(seed, value));
    }
    std::size_t operator()(const UInt256 & value) const {
        return PhHashMixSeed<sizeof(size_t), seed>()(hash_256(seed, value));
    }
};
