class Runable
	def on_finish(&callback)
		@on_finish = callback
		self
	end

	def run
		Thread.new do
			begin
				yield
			rescue Interrupt
			ensure
				@on_finish.call if @on_finish
			end
		end
	end
end

class Lister < Runable
	def initialize(bucket, key_queue, fetch_size)
		@bucket = bucket
		@key_queue = key_queue
		@fetch_size = fetch_size
	end

	def on_keys_chunk(&callback)
		@on_keys_chunk = callback
		self
	end
	
	def run
		super do
			marker = ''
			loop do
				keys_chunk = @bucket.keys('max-keys' => @fetch_size, marker: marker)
				break if keys_chunk.empty?
				@on_keys_chunk.call(keys_chunk) if @on_keys_chunk
				keys_chunk.each do |key|
					@key_queue << key
				end
				marker = keys_chunk.last.name
			end
		end
	end
end

class Worker < Runable
	def initialize(no, key_queue, &process_key)
		@no = no
		@key_queue = key_queue
		@process_key = process_key
	end

	def run
		super do
			until (key = @key_queue.pop) == :end
				@process_key.call(key)
			end
		end
	end
end

