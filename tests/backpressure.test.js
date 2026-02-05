import { describe, test, expect } from 'bun:test';
import { Writable, Readable } from 'stream';
import StreamCache from '../index.js';

/**
 * Helper to create a slow writable stream that simulates backpressure
 */
function createSlowWritableStream(onComplete) {
  let totalBytes = 0;
  let writeCount = 0;
  const maxBufferSize = 16 * 1024; // 16KB buffer
  let currentBufferSize = 0;

  const stream = new Writable({
    highWaterMark: maxBufferSize,
    write(chunk, encoding, callback) {
      writeCount++;
      totalBytes += chunk.length;
      currentBufferSize += chunk.length;

      // Simulate slow processing to trigger backpressure
      if (currentBufferSize >= maxBufferSize) {
        currentBufferSize = 0;
        // Small delay to simulate real-world processing
        setImmediate(() => callback());
      } else {
        callback();
      }
    },
    final(callback) {
      if (onComplete) {
        onComplete(totalBytes, writeCount);
      }
      callback();
    }
  });

  return stream;
}

/**
 * Helper to create a collector that tracks chunks
 */
function createChunkTrackingStream() {
  const chunks = [];
  let ended = false;

  const stream = new Writable({
    write(chunk, encoding, callback) {
      chunks.push({
        size: chunk.length,
        timestamp: Date.now()
      });
      callback();
    },
    final(callback) {
      ended = true;
      callback();
    }
  });

  stream.getChunks = () => chunks;
  stream.isEnded = () => ended;

  return stream;
}

describe('Backpressure handling', () => {
  describe('Large stream replay without freeze', () => {
    test('should handle 100 MB stream without blocking', (done) => {
      const cache = new StreamCache();
      const size100MB = 100 * 1024 * 1024;
      
      // Create a 100 MB buffer filled with pattern
      const largeBuffer = Buffer.alloc(size100MB, 'A');
      
      // Write to cache and end
      cache.write(largeBuffer);
      cache.end();
      
      let totalReceived = 0;
      let chunkCount = 0;
      const startTime = Date.now();
      let maxBlockTime = 0;
      let lastTime = startTime;
      
      const dest = new Writable({
        write(chunk, encoding, callback) {
          chunkCount++;
          totalReceived += chunk.length;
          
          // Measure time between chunks
          const now = Date.now();
          const blockTime = now - lastTime;
          maxBlockTime = Math.max(maxBlockTime, blockTime);
          lastTime = now;
          
          callback();
        },
        final(callback) {
          const endTime = Date.now();
          const duration = endTime - startTime;
          
          // Verify all data was received
          expect(totalReceived).toBe(size100MB);
          
          // Verify data was sent in multiple chunks (not one big chunk)
          expect(chunkCount).toBeGreaterThan(1);
          
          // Verify no single chunk blocked for more than 50ms
          // This ensures async processing is working
          expect(maxBlockTime).toBeLessThan(50);
          
          callback();
          done();
        }
      });
      
      // Pipe the cached large stream
      cache.pipe(dest);
    }, 30000); // 30 second timeout for this test

    test('should respect backpressure from slow destination', (done) => {
      const cache = new StreamCache();
      const size10MB = 10 * 1024 * 1024;
      
      // Create a 10 MB buffer
      const largeBuffer = Buffer.alloc(size10MB, 'B');
      
      cache.write(largeBuffer);
      cache.end();
      
      const dest = createSlowWritableStream((totalBytes, writeCount) => {
        expect(totalBytes).toBe(size10MB);
        expect(writeCount).toBeGreaterThan(1);
        done();
      });
      
      cache.pipe(dest);
    }, 30000);

    test('should send data in reasonable chunk sizes', (done) => {
      const cache = new StreamCache();
      const size5MB = 5 * 1024 * 1024;
      
      const buffer = Buffer.alloc(size5MB, 'C');
      
      cache.write(buffer);
      cache.end();
      
      const dest = createChunkTrackingStream();
      
      // Listen for finish event instead of using arbitrary timeout
      dest.on('finish', () => {
        const chunks = dest.getChunks();
        expect(dest.isEnded()).toBe(true);
        expect(chunks.length).toBeGreaterThan(1);
        
        // Verify chunks are reasonably sized (should be around 64KB each)
        const avgChunkSize = size5MB / chunks.length;
        
        // Most chunks should be around 64KB (allow some variance)
        expect(avgChunkSize).toBeGreaterThan(32 * 1024); // At least 32KB
        expect(avgChunkSize).toBeLessThanOrEqual(64 * 1024); // At most 64KB
        
        done();
      });
      
      cache.pipe(dest);
    }, 10000);
  });

  describe('Empty and small buffers', () => {
    test('should handle empty buffer after end', (done) => {
      const cache = new StreamCache();
      cache.end();
      
      let writeCount = 0;
      const dest = new Writable({
        write(chunk, encoding, callback) {
          writeCount++;
          callback();
        },
        final(callback) {
          expect(writeCount).toBe(0);
          callback();
        }
      });
      
      dest.on('finish', done);
      
      cache.pipe(dest);
    });

    test('should handle small buffer (less than chunk size)', (done) => {
      const cache = new StreamCache();
      const smallBuffer = Buffer.from('Small test data');
      
      cache.write(smallBuffer);
      cache.end();
      
      let received = Buffer.alloc(0);
      const dest = new Writable({
        write(chunk, encoding, callback) {
          received = Buffer.concat([received, chunk]);
          callback();
        },
        final(callback) {
          expect(received.toString()).toBe('Small test data');
          callback();
        }
      });
      
      dest.on('finish', done);
      
      cache.pipe(dest);
    });
  });

  describe('Destination becomes unwritable', () => {
    test('should stop writing when destination becomes unwritable', (done) => {
      const cache = new StreamCache();
      const size1MB = 1024 * 1024;
      
      const buffer = Buffer.alloc(size1MB, 'D');
      
      cache.write(buffer);
      cache.end();
      
      let writeCount = 0;
      let stopWriting = false;
      
      const dest = new Writable({
        write(chunk, encoding, callback) {
          writeCount++;
          
          if (writeCount === 3) {
            // Simulate stream becoming unwritable
            stopWriting = true;
            dest.writable = false;
          }
          
          callback();
        }
      });
      
      cache.pipe(dest);
      
      // Use a reasonable timeout to check that writing stopped
      setTimeout(() => {
        expect(stopWriting).toBe(true);
        // Should have stopped writing after a few chunks (well under total possible chunks)
        expect(writeCount).toBeLessThan(20);
        done();
      }, 500);
    });
  });

  describe('Async replay maintains correctness', () => {
    test('should deliver complete data correctly', (done) => {
      const cache = new StreamCache();
      const testData = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.repeat(10000);
      const buffer = Buffer.from(testData);
      
      cache.write(buffer);
      cache.end();
      
      let received = Buffer.alloc(0);
      const dest = new Writable({
        write(chunk, encoding, callback) {
          received = Buffer.concat([received, chunk]);
          callback();
        },
        final(callback) {
          expect(received.toString()).toBe(testData);
          expect(received.length).toBe(buffer.length);
          callback();
        }
      });
      
      dest.on('finish', done);
      
      cache.pipe(dest);
    });

    test('should handle multiple simultaneous pipes after end', (done) => {
      const cache = new StreamCache();
      const size2MB = 2 * 1024 * 1024;
      const buffer = Buffer.alloc(size2MB, 'E');
      
      cache.write(buffer);
      cache.end();
      
      const results = [];
      let completed = 0;
      
      for (let i = 0; i < 3; i++) {
        let received = 0;
        const dest = new Writable({
          write(chunk, encoding, callback) {
            received += chunk.length;
            callback();
          },
          final(callback) {
            results.push(received);
            completed++;
            
            if (completed === 3) {
              // All three destinations should receive complete data
              expect(results).toEqual([size2MB, size2MB, size2MB]);
              done();
            }
            
            callback();
          }
        });
        
        cache.pipe(dest);
      }
    }, 10000);
  });
});
