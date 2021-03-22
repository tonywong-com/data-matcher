require 'csv'
require 'fileutils'
require 'tempfile'
require 'pp'

class MatcherService
  attr_reader :options,
              :field_to_uid_maps,
              :row_cache,
              :auto_increment_uid,
              :total_rows,
              :total_duplicates

  UNIQUE_ID_FIELD_NAME = 'UID'
  OUTPUT_DATA_PATH = './data/'
  OUTPUT_FILE_SUFFIX = '_processed'
  PROGRESS_BAR_FREQUENCY = 100   # how many rows to process before printing a dot character.

  def initialize(options)
    @options = options
    # The purpose of this hash-table map is to provide O(1) time complexity for field matching.
    # So we can avoid the typical nested-loop O(n^2) algorithm.
    # The overall time complexity of the run() method is O(n).  It only requires
    # a sequential scan of the CSV file and a contant number of read/write's to the hash-table.
    # Since we have to load the whole hash-table into memory.  This algorithm is memory-bound
    # by the hash-table size.
    init_lookup_maps

    # A cache for the current row to reduce redundant processing.
    # {<field_index>: {field_type: <...>, normalized_field_value: <...>}}
    @row_cache = {}

    # To support the assignment of new uid
    init_auto_increment_uid
  end

  def run
    # Build paths
    input_path = @options.dig(:path)
    input_filename = File.basename(input_path)
    output_filename = File.basename(input_path, ".*") + OUTPUT_FILE_SUFFIX + File.extname(input_path)
    output_path = "#{OUTPUT_DATA_PATH}#{output_filename}"

    temp_file = Tempfile.new('csv')
    init_lookup_maps
    init_stats

    # Instead of reading a large CSV into memory, we use a memory-scalable approach by iterating
    # the CSV line-by-line and write to a temp file and then move final output to destination.
    CSV.open(temp_file, 'w') do |output_row|
      CSV.foreach(input_path).with_index(0) do |input_row, row_num|
        if row_num <= 0
          process_header_row(input_row, output_row)
        else
          process_data_row(input_row, output_row, row_num)
        end
        print_progress(row_num)
      end
    end
    puts

    # Move file from temp to output folder
    FileUtils.mv(temp_file, output_path, force: true)

    # Return stats
    { output_path: output_path,
      total_rows: @total_rows,
      total_duplicates: @total_duplicates,
      total_uids:  @auto_increment_uid - 1 }
  end

  private

  def init_lookup_maps
    @field_to_uid_maps = {}
    field_types = @options.dig(:match_types)
    field_types.each do |field_type|
      @field_to_uid_maps[field_type] = {}
    end
  end

  def init_stats
    @total_rows = 0
    @total_duplicates = 0
    init_auto_increment_uid
  end

  def init_auto_increment_uid
    @auto_increment_uid = 1
  end

  def get_new_uid
    new_uid = @auto_increment_uid
    @auto_increment_uid += 1
    new_uid
  end

  def process_header_row(input_row, output_row)
    # Decide which fields are used for matching
    init_row_cache(input_row)
    # Prepend UID column
    output_row << [UNIQUE_ID_FIELD_NAME] + input_row
  end

  def process_data_row(input_row, output_row, row_num)
    # Normalzie data first
    normalize_data_fields(input_row)

    # Data matching
    uid = nil
    uid = process_data_fields(input_row, uid)
    if uid.nil?
      # this is a new record
      uid = get_new_uid
    else
      # this is a duplicate
      @total_duplicates += 1
    end

    # Update the lookup map
    update_lookup_maps(input_row, uid)

    if @options.dig(:debug)
      puts "Processed row # #{row_num}"
      pp @row_cache
      pp @field_to_uid_maps
    end

    # Update stats
    @total_rows += 1

    # Prepend uid to first column
    output_row << [uid] + input_row
  end

  def update_lookup_maps(input_row, uid)
    @row_cache.each do |field_index, field_cache|
      field_value = input_row.dig(field_index)
      field_type = field_cache.dig(:field_type)
      normalized_field_value = field_cache.dig(:normalized_field_value)
      @field_to_uid_maps[field_type][normalized_field_value] = uid unless normalized_field_value.nil?
    end
  end

  def process_data_fields(input_row, uid)
    @row_cache.each do |field_index, field_cache|
      field_value = input_row.dig(field_index)
      field_type = field_cache.dig(:field_type)
      normalized_field_value = field_cache.dig(:normalized_field_value)
      # Look up previously seen value from the map
      uid = @field_to_uid_maps.dig(field_type, normalized_field_value)
      # Break if uid is found, which means this row is a duplicate.
      break unless uid.nil?
    end
    uid
  end

  def normalize_field_value(field_value, field_type)
    case field_type
    when 'email'
      normalized_field_value = field_value&.strip&.downcase
    when 'phone'
      normalized_field_value = normalize_phone_number(field_value)
    else
      normalized_field_value = field_value&.strip&.downcase
    end
    normalized_field_value
  end

  def normalize_phone_number(phone_num)
    # Delete all non-digit characters
    normalized_phone_num = phone_num&.delete('^0-9')

    # To keep the number consistent, we are prepending
    # internatinoal "1" code to all phone numbers with
    # 10 digits.  If the phone number has fewer than
    # 10 or more than 11 digits, we assume the phone
    # number is wrong, and do not normalize it any further.
    normalized_phone_num = "1" + normalized_phone_num if normalized_phone_num&.length == 10
    normalized_phone_num
  end

  def init_row_cache(header_row)
    # email, phone or both
    match_types = options[:match_types]

    header_row.each_with_index do |field_name, index|
      # Consider Email* field_name => email field_type
      # Consider Phone* field_name => phone field_type
      field_type = get_field_type_by_field_name(field_name, match_types)
      if !field_type.nil?
        @row_cache[index] = {
          field_type: field_type,
          normalized_field_value: nil
        }
      end
    end

    # Stop processing if there is no field to match
    raise "match_types not found in CSV header: #{match_types}" if @row_cache.empty?
  end

  def normalize_data_fields(input_row)
    @row_cache.each do |field_index, field_cache|
      field_value = input_row.dig(field_index)
      field_type = field_cache.dig(:field_type)
      field_cache[:normalized_field_value] = normalize_field_value(field_value, field_type)
    end
    @row_cache
  end

  def get_field_type_by_field_name(field_name, match_types)
    # Check if field_name is one of the match type
    field_type = field_name.gsub(/ *\d+$/, '').downcase
    if match_types.include?(field_type)
      field_type
    else
      nil
    end
  end

  def print_progress(row_num)
    print "." if (row_num + 1) % PROGRESS_BAR_FREQUENCY == 0
  end

end