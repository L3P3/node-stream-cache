"use strict";

class StreamCache extends require('stream').Stream{
	constructor(){
		super();
		
		this.writable=true;
		this.readable=true;
		
		this.length=0;
		
		//contains just one buffer if ended
		this._buffers=[];
		//is set to null if ended
		this._dests=[];
	}
	
	write(buffer){
		const dests=this._dests;
		if(dests!==null){}
		else
			throw Error('stream already ended');
		
		if(buffer.constructor===Buffer){}
		else
			throw Error('buffer expected');
		
		this._buffers.push(buffer);
		this.length+=buffer.length;
		
		var i=dests.length;
		while(i--)
			dests[i].write(buffer);
		
		return true;
	}
	
	end(buffer){
		const dests=this._dests;
		if(buffer===undefined){
			if(dests!==null){}
			else
				throw Error('stream already ended');
		}
		else
			this.write(buffer);
		
		var i=dests.length;
		while(i--)
			dests[i].end();
		
		this._dests=null;
		
		//merge all buffers into one since there will be no more being added
		//(saves memory and loopings in future pipe calls)
		this._buffers=[
			Buffer.concat(this._buffers)
		];
		
		return this;
	}
	
	pipe(dest,options){
		if(options===undefined){}
		else
			throw Error('options not supported');
		
		const buffers=this._buffers,dests=this._dests;
		if(dests===null){
			// Stream has ended, replay cached data asynchronously in chunks
			const buffer = buffers[0];
			if(buffer && buffer.length > 0){
				this._replayAsync(dest, buffer);
			} else {
				// Empty cache, just end the destination
				dest.end();
			}
			return dest;
		}
		
		for(var i=0,l=buffers.length;i<l;++i)
			dest.write(buffers[i]);
		
		dests.push(dest);
		
		return dest;
	}
	
	/**
	 * Asynchronously replay cached data to a destination stream.
	 * Sends data in chunks to prevent blocking I/O and respects backpressure.
	 * 
	 * @param {Writable} dest - The destination stream to write to
	 * @param {Buffer} buffer - The cached data buffer to replay
	 * @private
	 */
	_replayAsync(dest, buffer){
		// Chunk size for async replay (64KB by default)
		const chunkSize = 64 * 1024;
		let offset = 0;
		
		const writeChunk = () => {
			// Check if destination is still writable
			if(!dest.writable){
				return;
			}
			
			if(offset >= buffer.length){
				// All data sent, end the stream
				dest.end();
				return;
			}
			
			// Calculate chunk to send
			const end = Math.min(offset + chunkSize, buffer.length);
			const chunk = buffer.slice(offset, end);
			offset = end;
			
			// Write chunk and handle backpressure
			const canContinue = dest.write(chunk);
			
			if(canContinue){
				// No backpressure, schedule next chunk on next tick
				setImmediate(writeChunk);
			} else {
				// Backpressure detected, wait for drain event
				dest.once('drain', writeChunk);
			}
		};
		
		// Start writing chunks asynchronously
		setImmediate(writeChunk);
	}
	
	//deprecated
	getLength(){
		return this.length;
	}
}

module.exports=StreamCache;
