/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#include "util/aeron_platform.h"

#include <stdlib.h>
#include <errno.h>
#include <inttypes.h>

#if defined(AERON_COMPILER_MSVC)
#include <windows.h>
#endif

#include "util/aeron_bitutil.h"
#include "util/aeron_error.h"
#include "aeron_alloc.h"

int aeron_alloc(void **ptr, size_t size)
{
    void *bytes = malloc(size);
    if (NULL == bytes)
    {
        *ptr = NULL;
        AERON_SET_ERR(ENOMEM, "Failed to allocate %" PRIu64 " bytes", (uint64_t)size);
        return -1;
    }

    memset(bytes, 0, size);
    *ptr = bytes;

    return 0;
}

int aeron_alloc_aligned(void **ptr, size_t size, size_t alignment)
{
    if (!(AERON_IS_POWER_OF_TWO(alignment)))
    {
        AERON_SET_ERR(EINVAL, "Alignment must be a power of two: %" PRIu64, (uint64_t)alignment);
        return -1;
    }

    void* bytes = NULL;
    size_t aligned_size = AERON_ALIGN(size, alignment);

#if defined(AERON_COMPILER_MSVC)
    bytes = _aligned_malloc(aligned_size, alignment);
#else
    bytes = aligned_alloc(alignment, aligned_size);
#endif

    if (NULL == bytes)
    {
        *ptr = NULL;
        AERON_SET_ERR(ENOMEM, "Failed to allocate %" PRIu64 " bytes, alignment %" PRIu64, (uint64_t)size, (uint64_t)alignment);
        return -1;
    }

    memset(bytes, 0, size);
    *ptr = bytes;

    return 0;
}

#if defined(__linux__) || defined(AERON_COMPILER_MSVC)
int aeron_reallocf(void **ptr, size_t size)
{
    void *new_ptr = NULL;
    /* mimic reallocf */
    if (NULL == (new_ptr = realloc(*ptr, size)))
    {
        if (0 == size)
        {
            *ptr = NULL;
            return 0;
        }
        else
        {
            free(*ptr);
            *ptr = NULL;
            AERON_SET_ERR(ENOMEM, "Failed to re-allocate memory to a new size %" PRIu64, (uint64_t)size);
            return -1;
        }
    }

    *ptr = new_ptr;
    return 0;
}
#else
int aeron_reallocf(void **ptr, size_t size)
{
    if (NULL == (*ptr = reallocf(*ptr, size)))
    {
        if (0 != size)
        {
            AERON_SET_ERR(ENOMEM, "Failed to re-allocate memory to a new size %" PRIu64, (uint64_t)size);
            return -1;
        }
    }
    return 0;
}
#endif

void aeron_free(void *ptr)
{
    free(ptr);
}

#if defined(AERON_COMPILER_MSVC)
void aeron_free_aligned(void *ptr)
{
    _aligned_free(ptr);
}
#else
void aeron_free_aligned(void *ptr)
{
    free(ptr);
}
#endif
