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

#include <common/defines.h>

#include <cstring>

#if defined(__SSE2__)
#include <common/sse2_memcpy.h>
#endif
#if defined(__AVX2__)
#include <common/avx2_memcpy.h>
#endif

ALWAYS_INLINE static inline void * inline_memcpy(void * __restrict dst, const void * __restrict src, size_t size)
{
#if defined(__AVX2__)
    return mem_utils::avx2_inline_memcpy(dst, src, size);
#elif defined(__SSE2__)
    return sse2_inline_memcpy(dst, src, size);
#else
    return std::memcpy(dst, src, size);
#endif
}

ALWAYS_INLINE inline void memcpy_inlined(void* __restrict _dst, const void* __restrict _src, size_t size) {
  auto dst = static_cast<uint8_t*>(_dst);
  auto src = static_cast<const uint8_t*>(_src);

  [[maybe_unused]] tail : if (size <= 16) {
      if (size >= 8) {
          __builtin_memcpy(dst + size - 8, src + size - 8, 8);
          __builtin_memcpy(dst, src, 8);
      } else if (size >= 4) {
          __builtin_memcpy(dst + size - 4, src + size - 4, 4);
          __builtin_memcpy(dst, src, 4);
      } else if (size >= 2) {
          __builtin_memcpy(dst + size - 2, src + size - 2, 2);
          __builtin_memcpy(dst, src, 2);
      } else if (size >= 1) {
          *dst = *src;
      }
  }
  else {
#ifdef __AVX2__
      if (size <= 256) {
          if (size <= 32) {
              __builtin_memcpy(dst, src, 8);
              __builtin_memcpy(dst + 8, src + 8, 8);
              size -= 16;
              dst += 16;
              src += 16;
              goto tail;
          }

          while (size > 32) {
              _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst),
                                  _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src)));
              dst += 32;
              src += 32;
              size -= 32;
          }

          _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + size - 32),
                              _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + size - 32)));
      } else {
          static constexpr size_t KB = 1024;
          if (size >= 512 * KB && size <= 2048 * KB) {
              // erms(enhanced repeat movsv/stosb) version works well in this region.
              asm volatile("rep movsb" : "=D"(dst), "=S"(src), "=c"(size) : "0"(dst), "1"(src), "2"(size) : "memory");
          } else {
              size_t padding = (32 - (reinterpret_cast<size_t>(dst) & 31)) & 31;

              if (padding > 0) {
                  __m256i head = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src));
                  _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst), head);
                  dst += padding;
                  src += padding;
                  size -= padding;
              }

              while (size >= 256) {
                  __m256i c0 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src));
                  __m256i c1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 32));
                  __m256i c2 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 64));
                  __m256i c3 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 96));
                  __m256i c4 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 128));
                  __m256i c5 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 160));
                  __m256i c6 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 192));
                  __m256i c7 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 224));
                  src += 256;

                  _mm256_store_si256((reinterpret_cast<__m256i*>(dst)), c0);
                  _mm256_store_si256((reinterpret_cast<__m256i*>(dst + 32)), c1);
                  _mm256_store_si256((reinterpret_cast<__m256i*>(dst + 64)), c2);
                  _mm256_store_si256((reinterpret_cast<__m256i*>(dst + 96)), c3);
                  _mm256_store_si256((reinterpret_cast<__m256i*>(dst + 128)), c4);
                  _mm256_store_si256((reinterpret_cast<__m256i*>(dst + 160)), c5);
                  _mm256_store_si256((reinterpret_cast<__m256i*>(dst + 192)), c6);
                  _mm256_store_si256((reinterpret_cast<__m256i*>(dst + 224)), c7);
                  dst += 256;

                  size -= 256;
              }

              goto tail;
          }
      }
#else
      std::memcpy(dst, src, size);
#endif
  }
}
