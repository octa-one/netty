/*
 * Copyright 2024 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.channel.uring;

import java.nio.ByteBuffer;

/**
 * <pre>{@code
 * struct cmsghdr {
 *     socklen_t cmsg_len;    // data byte count, including header
 *     int cmsg_level;  //originating protocol
 *     int cmsg_type;   // protocol-specific type
 *     // followed by unsigned char cmsg_data[];
 * };
 * }</pre>
 */
final class CmsgHdr {

    private CmsgHdr() { }

    static void write(ByteBuffer cmsghdr, int cmsgHdrDataOffset,
                      int cmsgLen, int cmsgLevel, int cmsgType, short segmentSize) {
        int cmsghdrPosition = cmsghdr.position();
        if (Native.SIZEOF_SIZE_T == 4) {
            cmsghdr.putInt(cmsghdrPosition + Native.CMSG_OFFSETOF_CMSG_LEN, cmsgLen);
        } else {
            assert Native.SIZEOF_SIZE_T == 8;
            cmsghdr.putLong(cmsghdrPosition + Native.CMSG_OFFSETOF_CMSG_LEN, cmsgLen);
        }
        cmsghdr.putInt(cmsghdrPosition + Native.CMSG_OFFSETOF_CMSG_LEVEL, cmsgLevel);
        cmsghdr.putInt(cmsghdrPosition + Native.CMSG_OFFSETOF_CMSG_TYPE, cmsgType);
        cmsghdr.putShort(cmsghdrPosition + cmsgHdrDataOffset, segmentSize);
    }

    static void writeScmRights(ByteBuffer cmsghdr, int cmsgHdrDataOffset, int fd) {
        int cmsghdrPosition = cmsghdr.position();
        if (Native.SIZEOF_SIZE_T == 4) {
            cmsghdr.putInt(cmsghdrPosition + Native.CMSG_OFFSETOF_CMSG_LEN, Native.CMSG_LEN_FOR_FD);
        } else {
            assert Native.SIZEOF_SIZE_T == 8;
            cmsghdr.putLong(cmsghdrPosition + Native.CMSG_OFFSETOF_CMSG_LEN, Native.CMSG_LEN_FOR_FD);
        }
        cmsghdr.putInt(cmsghdrPosition + Native.CMSG_OFFSETOF_CMSG_LEVEL, Native.SOL_SOCKET);
        cmsghdr.putInt(cmsghdrPosition + Native.CMSG_OFFSETOF_CMSG_TYPE, Native.SCM_RIGHTS);
        cmsghdr.putInt(cmsghdrPosition + cmsgHdrDataOffset, fd);
    }

    static int readScmRights(ByteBuffer cmsghdr, int cmsgHdrDataOffset) {
        return cmsghdr.getInt(cmsghdr.position() + cmsgHdrDataOffset);
    }

    /**
     * Zero {@code cmsg_len}, which is all {@link #readUdpGroSegmentSize(ByteBuffer, int)}
     * needs to treat the cmsg as absent.
     */
    static void clearLen(ByteBuffer cmsghdr) {
        int cmsghdrPosition = cmsghdr.position();
        if (Native.SIZEOF_SIZE_T == 4) {
            cmsghdr.putInt(cmsghdrPosition + Native.CMSG_OFFSETOF_CMSG_LEN, 0);
        } else {
            assert Native.SIZEOF_SIZE_T == 8;
            cmsghdr.putLong(cmsghdrPosition + Native.CMSG_OFFSETOF_CMSG_LEN, 0);
        }
    }

    /**
     * Returns the segment size of a {@code UDP_GRO} cmsg, or {@code 0} if there is none.
     */
    static int readUdpGroSegmentSize(ByteBuffer cmsghdr, int cmsgHdrDataOffset) {
        int cmsghdrPosition = cmsghdr.position();
        long cmsgLen;
        if (Native.SIZEOF_SIZE_T == 4) {
            cmsgLen = cmsghdr.getInt(cmsghdrPosition + Native.CMSG_OFFSETOF_CMSG_LEN);
        } else {
            assert Native.SIZEOF_SIZE_T == 8;
            cmsgLen = cmsghdr.getLong(cmsghdrPosition + Native.CMSG_OFFSETOF_CMSG_LEN);
        }
        // Other control messages, enabled by options this class does not set, can take this slot,
        // so accept only a complete UDP_GRO cmsg here and nothing else may be read as a segment size.
        if (cmsgLen < Native.CMSG_LEN_FOR_UDP_GRO ||
                cmsghdr.getInt(cmsghdrPosition + Native.CMSG_OFFSETOF_CMSG_LEVEL) != Native.SOL_UDP ||
                cmsghdr.getInt(cmsghdrPosition + Native.CMSG_OFFSETOF_CMSG_TYPE) != Native.UDP_GRO) {
            return 0;
        }

        return cmsghdr.getInt(cmsghdrPosition + cmsgHdrDataOffset);
    }
}
