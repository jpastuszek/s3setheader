class Runable
	def on_finish(&callback)
		@on_finish = callback
		@thread = nil
		self
	end

	def run
		@thread = Thread.new do
			begin
				yield
			rescue Interrupt
			ensure
				@on_finish.call if @on_finish
			end
		end
		self
	end

	def join
		@thread.join if @thread
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

class Reporter < Runable
	class Collector
		def initialize(report_queue)
			@report_queue = report_queue
		end

		def on_finish(&callback)
			@on_finish = callback
			self
		end

		def run
			until (report = @report_queue.pop) == :end
				@sink.call(*report)
			end
			@on_finish.call if @on_finish
		end

		def each(&callback)
			@sink = callback
		end
	end

	def initialize(queue_size, &callback)
		@report_queue = SizedQueue.new(queue_size)
		on_finish do
			# flush thread waiting on queue
			@report_queue.max = 9999
		end

		@collector = Collector.new(@report_queue)
		@processor = callback
	end

	def on_report(&callback)
		@on_report = callback
	end

	def run
		super do
			break unless @processor
			@processor.call(@collector)
			@collector.run
		end
	end

	def report(key, value)
		@report_queue << [key, value]
	end

	def join
		@report_queue << :end
		super
	end
end

