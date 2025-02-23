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
#ifndef AERON_COMMAND_REMOVE_MESSAGE_FLYWEIGHT_H
#define AERON_COMMAND_REMOVE_MESSAGE_FLYWEIGHT_H

#include <cstdint>
#include <string>
#include <cstddef>
#include "CorrelatedMessageFlyweight.h"

namespace aeron { namespace command
{

/**
* Control message for removing a Publication or Subscription.
*
* <p>
*   0                   1                   2                   3
*   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
*  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
*  |                         Client ID                             |
*  |                                                               |
*  +---------------------------------------------------------------+
*  |                    Command Correlation ID                     |
*  |                                                               |
*  +---------------------------------------------------------------+
*  |                       Registration ID                         |
*  |                                                               |
*  +---------------------------------------------------------------+
*/

#pragma pack(push)
#pragma pack(4)
struct RemoveMessageDefn
{
    CorrelatedMessageDefn correlatedMessage;
    std::int64_t registrationId;
};
#pragma pack(pop)

class RemoveMessageFlyweight : public CorrelatedMessageFlyweight
{
public:
    inline RemoveMessageFlyweight(concurrent::AtomicBuffer &buffer, util::index_t offset) :
        CorrelatedMessageFlyweight(buffer, offset), m_struct(overlayStruct<RemoveMessageDefn>(0))
    {
    }

    inline std::int64_t registrationId() const
    {
        return m_struct.registrationId;
    }

    inline CorrelatedMessageFlyweight &registrationId(std::int64_t value)
    {
        m_struct.registrationId = value;
        return *this;
    }

    inline static util::index_t length()
    {
        return static_cast<util::index_t>(sizeof(RemoveMessageDefn));
    }

private:
    RemoveMessageDefn &m_struct;
};

}}
#endif
