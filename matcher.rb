require 'rubygems'
require 'optparse'
require 'byebug'

require_relative 'matcher_service'

class MatcherCommand
  attr_reader :option_parser,
              :arguments,
              :options

  MANDATORY_ARGUMENTS = [:path, :match_types]

  def initialize(arguments)
    @options = {}
    @option_parser = OptionParser.new()

    set_arguments(arguments)
    set_options
  end

  def execute

    begin
      puts "=" * 30
      puts "Start matching and grouping data, based on #{@options.dig(:match_types)}"
      puts "Options: #{@options}" if @options.dig(:debug)
      puts "=" * 30
      puts

      # Set timer
      start_time = Time.now

      matcher_service = MatcherService.new(@options)
      result = matcher_service.run

      puts
      puts "=" * 30
      puts "Finished matching and grouping data, based on #{@options.dig(:match_types)}"
      puts "Output data file can be found in '#{result.dig(:output_path)}'"
      puts "Unique uid's: #{result.dig(:total_uids)}"
      puts "Duplicate records: #{result.dig(:total_duplicates)}"
      puts "Total records: #{result.dig(:total_rows)}"
      puts "Total run time: #{Time.at(Time.now - start_time).utc.strftime("%H:%M:%S.%L")}";0
      puts "=" * 30

    rescue => exception
      puts
      puts "=" * 30
      puts "Error in matching and grouping data."
      puts "ERROR: #{exception.to_s}"
      puts "Backtrace =>\n\t" + exception.backtrace.join("\n\t") if options[:debug]
      puts "Total run time: #{Time.at(Time.now - start_time).utc.strftime("%H:%M:%S.%L")}";0
      puts "=" * 30
    end

  end

  private

  def set_arguments(arguments)
    return if arguments.nil?
    if(arguments.kind_of?(String))
      @arguments = arguments.split(/s{1,}/)
    elsif (arguments.kind_of?(Array))
      @arguments = arguments
    else
      raise "Expecting either String or an Array"
    end
  end

  def init_option_parser
    @option_parser.banner = "Usage: ruby #{File.basename(__FILE__)} [options]"

    # argument --debug
    @options[:debug] = false
    @option_parser.on('--[no-]debug','Print debug statements') do |opt|
      @options[:debug] = opt
    end

    # argument --path
    @options[:path] = nil
    @option_parser.on('-p','--path <path>','CSV file path') do |opt|
      @options[:path] = opt
    end

    # argument --path
    @options[:match_types] = nil
    @option_parser.on('-mt','--match_types <match_types_list>', Array, 'Match types (comma separated, no space)') do |opt|
      @options[:match_types] = opt
    end
  end

  def set_options
    init_option_parser
    begin

      # Parse arguments
      @option_parser.parse!(@arguments)
      # Validate for mandatory arguments
      missing_arguments = MANDATORY_ARGUMENTS.select{|arg| @options.dig(arg).nil?}
      raise OptionParser::MissingArgument, missing_arguments.join(', ') if missing_arguments.any?

    rescue OptionParser::ParseError,
      OptionParser::InvalidArgument,
      OptionParser::InvalidOption,
      OptionParser::MissingArgument => exception

      puts "Parsing Error: #{exception.to_s}"
      puts @option_parser
      exit

    end
  end

end

def logputs(message)
  timestamp = Time.now.strftime('%Y-%m-%d %H:%M:%S')
  puts "#{timestamp} - #{message}"
end

if __FILE__ == $0
  begin
    matcher_command = MatcherCommand.new(ARGV)
    matcher_command.execute
  rescue => exception
    logputs "ERROR: #{exception.to_s}"
    logputs "Backtrace =>\n\t" + exception.backtrace.join("\n\t")
  end
end