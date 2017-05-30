/*
 * Copyright 2014-2017 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.archiver;

import io.aeron.archiver.codecs.RecordingDescriptorDecoder;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.*;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static io.aeron.archiver.ArchiveUtil.*;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.logbuffer.FrameDescriptor.PADDING_FRAME_TYPE;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.file.StandardOpenOption.READ;

class RecordingFragmentReader implements AutoCloseable
{
    interface SimplifiedControlledPoll
    {
        /**
         * Called by the {@link RecordingFragmentReader}. Implementors need only process DATA fragments.
         *
         * @return true if fragment processed, false to abort.
         */
        boolean onFragment(
            DirectBuffer fragmentBuffer,
            int fragmentOffset,
            int fragmentLength,
            DataHeaderFlyweight header);
    }

    private static final long NULL_POSITION = -1;
    private static final long NULL_LENGTH = -1;

    private final long recordingId;
    private final File archiveDir;
    private final int termBufferLength;
    private final long replayLength;
    private final int segmentFileLength;
    private final long fromPosition;

    private int segmentFileIndex;
    private UnsafeBuffer termBuffer = null;
    private int recordingTermStartOffset;
    private int termOffset;
    private long transmitted = 0;
    private final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight();
    private MappedByteBuffer mappedByteBuffer;

    RecordingFragmentReader(final long recordingId, final File archiveDir) throws IOException
    {
        this(getDescriptor(recordingId, archiveDir), recordingId, archiveDir, NULL_POSITION, NULL_LENGTH);
    }

    RecordingFragmentReader(
        final long recordingId,
        final File archiveDir,
        final long position,
        final long length) throws IOException
    {
        this(getDescriptor(recordingId, archiveDir), recordingId, archiveDir, position, length);
    }

    public void close()
    {
        closeRecordingFile();
    }

    private RecordingFragmentReader(
        final RecordingDescriptorDecoder metaDecoder,
        final long recordingId,
        final File archiveDir,
        final long position,
        final long length) throws IOException
    {
        this.recordingId = recordingId;
        this.archiveDir = archiveDir;
        termBufferLength = metaDecoder.termBufferLength();
        segmentFileLength = metaDecoder.segmentFileLength();
        final long recordingLength = ArchiveUtil.recordingLength(metaDecoder);
        final long joiningPosition = metaDecoder.joiningPosition();

        replayLength = length == NULL_LENGTH ? recordingLength : length;

        final long fromPosition = position == NULL_POSITION ? joiningPosition : position;
        segmentFileIndex = segmentFileIndex(joiningPosition, fromPosition, segmentFileLength);
        final long recordingOffset = fromPosition & (segmentFileLength - 1);
        openRecordingFile();

        recordingTermStartOffset = (int)(recordingOffset - (recordingOffset & (termBufferLength - 1)));
        termBuffer = new UnsafeBuffer(mappedByteBuffer, recordingTermStartOffset, termBufferLength);
        termOffset = (int)(recordingOffset & (termBufferLength - 1));

        // TODO: Test for starting position being correct with termId and termOffset of first frame.

        int frameOffset = 0;
        while (frameOffset < termOffset)
        {
            frameOffset += termBuffer.getInt(frameOffset + DataHeaderFlyweight.FRAME_LENGTH_FIELD_OFFSET);
        }

        if (frameOffset != termOffset)
        {
            this.fromPosition = fromPosition + (frameOffset - termOffset);
        }
        else
        {
            this.fromPosition = fromPosition;
        }

        if (frameOffset >= termBufferLength)
        {
            termOffset = 0;
            nextTerm();
        }
        else
        {
            termOffset = frameOffset;
        }
    }

    boolean isDone()
    {
        return transmitted >= replayLength;
    }

    long fromPosition()
    {
        return fromPosition;
    }

    int controlledPoll(final SimplifiedControlledPoll fragmentHandler, final int fragmentLimit)
        throws IOException
    {
        if (isDone())
        {
            return 0;
        }

        int polled = 0;

        // read to end of term or requested data
        while (termOffset < termBufferLength && !isDone() && polled < fragmentLimit)
        {
            final int frameOffset = termOffset;
            headerFlyweight.wrap(termBuffer, frameOffset, DataHeaderFlyweight.HEADER_LENGTH);
            final int frameLength = headerFlyweight.frameLength();
            final int alignedLength = BitUtil.align(frameLength, FRAME_ALIGNMENT);

            // cursor moves forward, importantly an exception from onFragment will not block progress
            transmitted += alignedLength;
            termOffset += alignedLength;

            final int fragmentDataOffset = frameOffset + DataHeaderFlyweight.DATA_OFFSET;
            final int fragmentDataLength = frameLength - DataHeaderFlyweight.HEADER_LENGTH;

            if (!fragmentHandler.onFragment(
                termBuffer,
                fragmentDataOffset,
                fragmentDataLength,
                headerFlyweight))
            {
                // rollback the cursor progress
                transmitted -= alignedLength;
                termOffset -= alignedLength;
                return polled;
            }

            // only count data fragments, consistent with sent fragment count
            if (headerFlyweight.headerType() != PADDING_FRAME_TYPE)
            {
                polled++;
            }
        }

        if (!isDone() && termOffset == termBufferLength)
        {
            termOffset = 0;
            nextTerm();
        }

        return polled;
    }

    private void nextTerm() throws IOException
    {
        recordingTermStartOffset += termBufferLength;

        if (recordingTermStartOffset == segmentFileLength)
        {
            closeRecordingFile();
            segmentFileIndex++;
            openRecordingFile();
        }

        termBuffer.wrap(mappedByteBuffer, recordingTermStartOffset, termBufferLength);
    }

    private void closeRecordingFile()
    {
        IoUtil.unmap(mappedByteBuffer);
    }

    private void openRecordingFile() throws IOException
    {
        recordingTermStartOffset = 0;
        final String recordingDataFileName = recordingDataFileName(recordingId, segmentFileIndex);
        final File recordingDataFile = new File(archiveDir, recordingDataFileName);

        try (FileChannel fileChannel = FileChannel.open(recordingDataFile.toPath(), READ))
        {
            mappedByteBuffer = fileChannel.map(READ_ONLY, 0, segmentFileLength);
        }
    }

    private static RecordingDescriptorDecoder getDescriptor(final long recordingId, final File archiveDir)
        throws IOException
    {
        final String recordingMetaFileName = recordingMetaFileName(recordingId);
        final File recordingMetaFile = new File(archiveDir, recordingMetaFileName);
        return loadRecordingDescriptor(recordingMetaFile);
    }
}
