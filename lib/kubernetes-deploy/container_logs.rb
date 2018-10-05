# frozen_string_literal: true
module KubernetesDeploy
  class ContainerLogs
    attr_reader :lines, :container_name

    DEFAULT_LINE_LIMIT = 250

    def initialize(parent_id:, container_name:, logger:)
      @parent_id = parent_id
      @container_name = container_name
      @logger = logger
      @lines = []
      @last_printed_index = -1
    end

    def sync(kubectl)
      new_logs = fetch_latest(kubectl)
      return unless new_logs.present?
      @lines += deduplicate(new_logs)
    end

    def empty?
      lines.empty?
    end

    def print_latest(prefix: false)
      prefix_str = "[#{container_name}]  " if prefix
      start_at = @last_printed_index + 1

      lines[start_at..-1].each do |msg|
        @logger.info "#{prefix_str}#{msg}"
      end

      @last_printed_index = lines.length - 1
    end

    def print_all
      lines.each { |line| @logger.info("\t#{line}") }
    end

    private

    def fetch_latest(kubectl)
      cmd = ["logs", @parent_id, "--container=#{container_name}", "--timestamps"]
      cmd << if @last_timestamp.present?
        "--since-time=#{rfc3339_timestamp(@last_timestamp)}"
      else
        "--tail=#{DEFAULT_LINE_LIMIT}"
      end
      out, _err, _st = kubectl.run(*cmd, log_failure: false)
      out.split("\n")
    end

    def rfc3339_timestamp(time)
      time.strftime("%FT%T.%N%:z")
    end

    def deduplicate(logs)
      deduped = []
      timestamps = []

      logs.each do |line|
        timestamp, msg = split_timestamped_line(line)
        next if likely_duplicate?(timestamp)
        timestamps << timestamp if timestamp
        deduped << msg
      end

      @last_timestamp = timestamps.max
      deduped
    end

    def split_timestamped_line(log_line)
      timestamp, message = log_line.split(" ", 2)
      [Time.parse(timestamp), message]
    rescue ArgumentError
      # If the log file can't be opened, k8s 1.8 writes an error message without a timestamp to stdout
      [nil, log_line]
    end

    def likely_duplicate?(timestamp)
      return false unless @last_timestamp && timestamp
      # The --since-time granularity the API server supports is not adequate to prevent duplicates
      # This comparison takes the fractional seconds into account
      timestamp <= @last_timestamp
    end
  end
end
