import { describe, test, expect } from 'bun:test';
import { Writable, Readable } from 'stream';
import StreamCache from '../index.js';

/**
 * Helper to create a writable stream that collects data
 */
function createCollectorStream() {
  const chunks = [];
  let ended = false;

  const stream = new Writable({
    write(chunk, encoding, callback) {
      chunks.push(chunk);
      callback();
    },
    final(callback) {
      ended = true;
      callback();
    }
  });

  stream.getChunks = () => chunks;
  stream.getData = () => Buffer.concat(chunks);
  stream.isEnded = () => ended;

  return stream;
}

/**
 * Helper to create a readable stream that emits data
 */
function createSourceStream(data) {
  let index = 0;
  
  return new Readable({
    read() {
      if (index < data.length) {
        this.push(Buffer.from(data[index]));
        index++;
      } else {
        this.push(null);
      }
    }
  });
}

describe('StreamCache', () => {
  describe('Basic functionality', () => {
    test('should create a StreamCache instance', () => {
      const cache = new StreamCache();
      expect(cache).toBeDefined();
      expect(cache.writable).toBe(true);
      expect(cache.readable).toBe(true);
      expect(cache.length).toBe(0);
    });

    test('should write and cache data', () => {
      const cache = new StreamCache();
      const buffer = Buffer.from('Hello');
      
      cache.write(buffer);
      
      expect(cache.length).toBe(5);
    });

    test('should throw error when writing non-buffer', () => {
      const cache = new StreamCache();
      
      expect(() => {
        cache.write('not a buffer');
      }).toThrow('buffer expected');
    });

    test('should throw error when writing after end', () => {
      const cache = new StreamCache();
      cache.end();
      
      expect(() => {
        cache.write(Buffer.from('test'));
      }).toThrow('stream already ended');
    });

    test('should throw error when calling end twice', () => {
      const cache = new StreamCache();
      cache.end();
      
      expect(() => {
        cache.end();
      }).toThrow('stream already ended');
    });
  });

  describe('Test case 1: Stream is still running', () => {
    test('should pipe data while stream is running', (done) => {
      const cache = new StreamCache();
      const dest = createCollectorStream();
      
      // Pipe to destination before data arrives
      cache.pipe(dest);
      
      // Simulate streaming data
      setTimeout(() => {
        cache.write(Buffer.from('Hello '));
      }, 10);
      
      setTimeout(() => {
        cache.write(Buffer.from('World'));
      }, 20);
      
      setTimeout(() => {
        cache.end();
        
        // Check that destination received all data
        expect(dest.getData().toString()).toBe('Hello World');
        expect(dest.isEnded()).toBe(true);
        done();
      }, 30);
    });

    test('should pipe new data to destination added while streaming', (done) => {
      const cache = new StreamCache();
      const dest1 = createCollectorStream();
      const dest2 = createCollectorStream();
      
      // First destination
      cache.pipe(dest1);
      
      cache.write(Buffer.from('Hello '));
      
      // Add second destination after some data has been written
      cache.pipe(dest2);
      
      cache.write(Buffer.from('World'));
      cache.end();
      
      setTimeout(() => {
        // dest1 should have all data
        expect(dest1.getData().toString()).toBe('Hello World');
        expect(dest1.isEnded()).toBe(true);
        
        // dest2 should have cached data + new data
        expect(dest2.getData().toString()).toBe('Hello World');
        expect(dest2.isEnded()).toBe(true);
        
        done();
      }, 10);
    });
  });

  describe('Test case 2: Cache is piped to multiple sinks', () => {
    test('should pipe to multiple destinations simultaneously', (done) => {
      const cache = new StreamCache();
      const dest1 = createCollectorStream();
      const dest2 = createCollectorStream();
      const dest3 = createCollectorStream();
      
      // Pipe to all destinations
      cache.pipe(dest1);
      cache.pipe(dest2);
      cache.pipe(dest3);
      
      // Write some data
      cache.write(Buffer.from('Test '));
      cache.write(Buffer.from('Data'));
      cache.end();
      
      setTimeout(() => {
        // All destinations should receive the same data
        expect(dest1.getData().toString()).toBe('Test Data');
        expect(dest2.getData().toString()).toBe('Test Data');
        expect(dest3.getData().toString()).toBe('Test Data');
        
        expect(dest1.isEnded()).toBe(true);
        expect(dest2.isEnded()).toBe(true);
        expect(dest3.isEnded()).toBe(true);
        
        done();
      }, 10);
    });

    test('should pipe to destinations added at different times', (done) => {
      const cache = new StreamCache();
      const dest1 = createCollectorStream();
      const dest2 = createCollectorStream();
      const dest3 = createCollectorStream();
      
      // Add first destination
      cache.pipe(dest1);
      cache.write(Buffer.from('Part1 '));
      
      // Add second destination
      cache.pipe(dest2);
      cache.write(Buffer.from('Part2 '));
      
      // Add third destination
      cache.pipe(dest3);
      cache.write(Buffer.from('Part3'));
      
      cache.end();
      
      setTimeout(() => {
        // Each destination should have cached data + subsequent data
        expect(dest1.getData().toString()).toBe('Part1 Part2 Part3');
        expect(dest2.getData().toString()).toBe('Part1 Part2 Part3');
        expect(dest3.getData().toString()).toBe('Part1 Part2 Part3');
        
        done();
      }, 10);
    });
  });

  describe('Test case 3: Stream ends and then piped to something again', () => {
    test('should replay cached data after stream ends', (done) => {
      const cache = new StreamCache();
      const dest1 = createCollectorStream();
      
      // Write data and end
      cache.write(Buffer.from('Cached '));
      cache.write(Buffer.from('Data'));
      cache.end();
      
      setTimeout(() => {
        // Pipe to destination after stream has ended
        const dest2 = createCollectorStream();
        cache.pipe(dest2);
        
        setTimeout(() => {
          // dest2 should immediately receive all cached data and end
          expect(dest2.getData().toString()).toBe('Cached Data');
          expect(dest2.isEnded()).toBe(true);
          
          done();
        }, 10);
      }, 10);
    });

    test('should replay to multiple destinations after stream ends', (done) => {
      const cache = new StreamCache();
      
      // Write and end the stream
      cache.write(Buffer.from('Complete '));
      cache.write(Buffer.from('Message'));
      cache.end();
      
      setTimeout(() => {
        // Pipe to multiple destinations after end
        const dest1 = createCollectorStream();
        const dest2 = createCollectorStream();
        const dest3 = createCollectorStream();
        
        cache.pipe(dest1);
        cache.pipe(dest2);
        cache.pipe(dest3);
        
        setTimeout(() => {
          // All destinations should receive complete cached data
          expect(dest1.getData().toString()).toBe('Complete Message');
          expect(dest2.getData().toString()).toBe('Complete Message');
          expect(dest3.getData().toString()).toBe('Complete Message');
          
          expect(dest1.isEnded()).toBe(true);
          expect(dest2.isEnded()).toBe(true);
          expect(dest3.isEnded()).toBe(true);
          
          done();
        }, 10);
      }, 10);
    });

    test('should handle empty cache after end', (done) => {
      const cache = new StreamCache();
      
      // End without writing any data
      cache.end();
      
      setTimeout(() => {
        const dest = createCollectorStream();
        cache.pipe(dest);
        
        setTimeout(() => {
          expect(dest.getData().length).toBe(0);
          expect(dest.isEnded()).toBe(true);
          
          done();
        }, 10);
      }, 10);
    });
  });

  describe('Integration with real streams', () => {
    test('should work with readable stream piped to cache', (done) => {
      const cache = new StreamCache();
      const source = createSourceStream(['Hello', ' ', 'World']);
      const dest = createCollectorStream();
      
      // Pipe source to cache and cache to destination
      source.pipe(cache);
      cache.pipe(dest);
      
      setTimeout(() => {
        expect(dest.getData().toString()).toBe('Hello World');
        expect(dest.isEnded()).toBe(true);
        
        done();
      }, 100);
    });

    test('should cache and replay with readable stream', (done) => {
      const cache = new StreamCache();
      const source = createSourceStream(['Test', ' ', 'Stream']);
      
      // Pipe source to cache
      source.pipe(cache);
      
      setTimeout(() => {
        // After source has ended, pipe to new destination
        const dest = createCollectorStream();
        cache.pipe(dest);
        
        setTimeout(() => {
          expect(dest.getData().toString()).toBe('Test Stream');
          expect(dest.isEnded()).toBe(true);
          
          done();
        }, 10);
      }, 100);
    });
  });

  describe('Edge cases', () => {
    test('should handle large buffers', (done) => {
      const cache = new StreamCache();
      const largeBuffer = Buffer.alloc(1024 * 1024); // 1MB
      largeBuffer.fill('a');
      
      cache.write(largeBuffer);
      expect(cache.length).toBe(1024 * 1024);
      
      cache.end();
      
      const dest = createCollectorStream();
      
      // Data is now delivered asynchronously, so we use event-based completion
      dest.on('finish', () => {
        expect(dest.getData().length).toBe(1024 * 1024);
        done();
      });
      
      cache.pipe(dest);
    });

    test('should handle end with buffer argument', (done) => {
      const cache = new StreamCache();
      const dest = createCollectorStream();
      
      cache.pipe(dest);
      cache.write(Buffer.from('Hello '));
      cache.end(Buffer.from('World'));
      
      setTimeout(() => {
        expect(dest.getData().toString()).toBe('Hello World');
        expect(dest.isEnded()).toBe(true);
        
        done();
      }, 10);
    });

    test('should not support pipe options', () => {
      const cache = new StreamCache();
      const dest = createCollectorStream();
      
      expect(() => {
        cache.pipe(dest, { end: false });
      }).toThrow('options not supported');
    });

    test('should support getLength() for backward compatibility', () => {
      const cache = new StreamCache();
      cache.write(Buffer.from('test'));
      
      expect(cache.getLength()).toBe(4);
      expect(cache.length).toBe(4);
    });
  });

  describe('Buffer consolidation', () => {
    test('should consolidate buffers after end', () => {
      const cache = new StreamCache();
      
      // Write multiple buffers
      cache.write(Buffer.from('A'));
      cache.write(Buffer.from('B'));
      cache.write(Buffer.from('C'));
      
      expect(cache._buffers.length).toBe(3);
      
      cache.end();
      
      // After end, buffers should be consolidated into one
      expect(cache._buffers.length).toBe(1);
      expect(cache._buffers[0].toString()).toBe('ABC');
    });
  });
});
