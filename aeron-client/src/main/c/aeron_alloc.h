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

#ifndef AERON_ALLOC_H
#define AERON_ALLOC_H

#include <stddef.h>

/**
 * Allocate a chunk of zeroed out memory.
 *
 * @param ptr to assign if allocation is successful.
 * @param size of the allocation.
 * @return zero upon success or -1 in case of an error.
 */
int aeron_alloc(void **ptr, size_t size);

/**
 * Allocate a chunk of zeroed out memory and align pointer to the specified alignment.
 *
 * @param ptr to assign if allocation is successful.
 * @param size of the allocation.
 * @param alignment for the pointer. Must be a power of two.
 * @return zero upon success or -1 in case of an error.
 */
int aeron_alloc_aligned(void **ptr, size_t size, size_t alignment);

/**
 * Changes the size of the memory block pointed to by ptr to size bytes. Will allocate an uninitialized memory if ptr
 * is null.
 *
 * <p><em>NOTE:</em> does not work on memory allocated with `aeron_alloc_aligned`, because Windows.</p>
 *
 * @param ptr to assign if re-allocation is successful.
 * @param size of the re-allocation.
 * @return zero upon success or -1 in case of an error.
 */
int aeron_reallocf(void **ptr, size_t size);

/**
 * Free memory that was allocated with `aeron_alloc` or `aeron_alloc_no_err`.
 * @param ptr to be freed.
 */
void aeron_free(void *ptr);

/**
 * Free memory that was allocated with `aeron_alloc_aligned`.
 * @param ptr to be freed.
 */
void aeron_free_aligned(void *ptr);

#endif //AERON_ALLOC_H
