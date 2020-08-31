module CukeForker
  class WorkerQueue
    include Observable

    def initialize(max, delay, fail_fast=false, max_duration = nil)
      @max = max
      @delay = delay
      @fail_fast = fail_fast
      @max_duration = max_duration

      if @max < 0
        raise ArgumentError, "max workers cannot be negative, got #{@max.inspect}"
      end

      unless @delay.kind_of?(Numeric)
        raise ArgumentError, "delay must be Numeric, got #{@delay.inspect}:#{@delay.class}"
      end

      @pending = []
      @running = []
      @finished = []
    end

    def backed_up?
      @pending.any?
    end

    def add(worker)
      @pending << worker
    end

    def process(poll_interval = nil)
      @start_time = Time.now

      while backed_up?
        fill
        eta
        poll poll_interval while full?
      end

      # yay, no more pending workers
    end

    def wait_until_finished(poll_interval = nil)
      until empty?
        poll poll_interval
        eta
      end
    end

    def fill
      while backed_up? and not full?
        worker = @pending.shift
        start worker
      end
    end

    def poll(seconds = nil)
      kill_long_test
      finished = @running.select { |w| w.finished? }

      if finished.empty?
        sleep seconds if seconds
      else
        finished.each { |w| finish w }
      end
    end

    def size
      @running.size
    end

    def full?
      @max != 0 && size == @max
    end

    def empty?
      @running.empty?
    end

    def has_failures?
      @finished.any? { |w| w.failed? }
    end

    def kill_long_test
      @running.each do |running_worker|
        execution_time = (Time.now.utc - running_worker.time).to_f.round(2)
        if execution_time > @max_duration
          Process.kill(1, running_worker.pid)
          finish running_worker, true
        end
      end
    end

    def eta
      pending  = @pending.size
      finished = @finished.size
      running  = @running.size

      remaining = pending + running
      sleep 3

      if finished == 0
        result = [Time.now, remaining, finished]
        fire :on_eta, *result
      else
        seconds_per_child = (Time.now - start_time) / finished.to_f
        eta = Time.now + (seconds_per_child * remaining)

        result = [eta, remaining, finished]

        fire :on_eta, *result
      end

      result
    end

    def add_observer(observer)
      @pending.each { |e| e.add_observer observer }
      super
    end

    private

    def start(worker)
      fire :on_worker_starting, worker

      worker.start
      @running << worker
      sleep @delay
    end

    def finish(worker, long = false)
      @running.delete worker
      @finished << worker

      if @fail_fast && worker.failed?
        @pending.clear
        @running.each { |w| w.kill }
        @running.clear
      end

      long ? fire(:long_worker_killed, worker) : fire(:on_worker_finished, worker)
    end

    def fire(*args)
      changed
      notify_observers(*args)
    end

    def start_time
      @start_time or raise NotStartedError
    end

    class NotStartedError < StandardError; end

  end # WorkerQueue
end # CukeForker
